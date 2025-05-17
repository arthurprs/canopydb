use std::{
    io,
    path::{Path, PathBuf},
};

use super::{FileSystem, FsFile};

use crate::{
    shim::sync::{Arc, Mutex},
    HashMap,
    HashSet,
};

#[derive(Debug)]
pub struct MemFile {
    data: Mutex<Vec<u8>>,
}

impl MemFile {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(Vec::new()),
        }
    }
}

impl FsFile for MemFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let data = self.data.lock().unwrap();
        let offset = offset as usize;
        if offset + buf.len() > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of file",
            ));
        }
        buf.copy_from_slice(&data[offset..offset + buf.len()]);
        Ok(())
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut data = self.data.lock().unwrap();
        let offset = offset as usize;
        let end = offset + buf.len();
        if end > data.len() {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(buf);
        Ok(())
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        let mut data = self.data.lock().unwrap();
        let size = size as usize;
        data.resize(size, 0);
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        let data = self.data.lock().unwrap();
        Ok(data.len() as u64)
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct MemFS {
    state: Mutex<MemFSState>,
}

#[derive(Default)]
struct MemFSState {
    files: HashMap<PathBuf, Arc<MemFile>>,
    dirs: HashSet<PathBuf>,
}

impl FileSystem for MemFS {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be unix-style absolute (start with /)",
            ));
        }
        let parent = path.parent().unwrap_or_else(|| Path::new("/"));
        let mut state = self.state.lock().unwrap();
        if !state.dirs.contains(parent) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "parent directory does not exist",
            ));
        }
        let file = state
            .files
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(MemFile::new()));
        Ok(file.clone())
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be unix-style absolute (start with /)",
            ));
        }
        let parent = path.parent().unwrap_or_else(|| Path::new("/"));
        let mut state = self.state.lock().unwrap();
        if !state.dirs.contains(parent) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "parent directory does not exist",
            ));
        }
        let file = Arc::new(MemFile::new());
        state.files.insert(path.to_path_buf(), file.clone());
        Ok(file)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be unix-style absolute (start with /)",
            ));
        }
        let mut state = self.state.lock().unwrap();
        if state.files.remove(path).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        }
        Ok(())
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if from.to_str().map(|s| s.starts_with('/')) != Some(true)
            || to.to_str().map(|s| s.starts_with('/')) != Some(true)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "paths must be unix-style absolute (start with /)",
            ));
        }
        let parent_to = to.parent().unwrap_or_else(|| Path::new("/"));
        let mut state = self.state.lock().unwrap();
        if !state.dirs.contains(parent_to) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "destination parent directory does not exist",
            ));
        }
        if state.dirs.contains(to) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "destination directory already exists",
            ));
        }
        if let Some(file) = state.files.remove(from) {
            state.files.insert(to.to_path_buf(), file);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
        }
    }
    fn exists(&self, path: &Path) -> bool {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return false;
        }
        let state = self.state.lock().unwrap();
        state.files.contains_key(path) || state.dirs.contains(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be unix-style absolute (start with /)",
            ));
        }
        let mut state = self.state.lock().unwrap();
        let mut cur = PathBuf::new();
        for comp in path.components() {
            cur.push(comp);
            if state.files.contains_key(&cur) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "file exists with same name as directory",
                ));
            }
            state.dirs.insert(cur.clone());
        }
        Ok(())
    }
    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be unix-style absolute (start with /)",
            ));
        }
        let mut state = self.state.lock().unwrap();
        state
            .files
            .retain(|file_path, _| !file_path.starts_with(path));
        state.dirs.retain(|dir_path| !dir_path.starts_with(path));
        Ok(())
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<String>> {
        if path.to_str().map(|s| s.starts_with('/')) != Some(true) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be unix-style absolute (start with /)",
            ));
        }
        let state = self.state.lock().unwrap();
        if !state.dirs.contains(path) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "directory does not exist",
            ));
        }
        let mut entries = Vec::new();
        for file in state.files.keys() {
            if let Some(parent) = file.parent() {
                if parent == path {
                    if let Some(name) = file.file_name().map(|s| s.to_string_lossy().to_string()) {
                        entries.push(name);
                    }
                }
            }
        }
        for dir in state.dirs.iter() {
            if dir.parent() == Some(path) {
                if let Some(name) = dir.file_name().map(|s| s.to_string_lossy().to_string()) {
                    entries.push(name);
                }
            }
        }
        Ok(entries)
    }
    fn sync_dir(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fs() -> MemFS {
        MemFS::default()
    }

    #[test]
    fn test_create_and_read_file() {
        let fs = fs();
        fs.create_dir_all(Path::new("/foo")).unwrap();
        let file = fs.create(Path::new("/foo/bar")).unwrap();
        file.write_all_at(b"hello", 0).unwrap();
        let mut buf = [0u8; 5];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");
        assert_eq!(file.len().unwrap(), 5);
    }

    #[test]
    fn test_file_overwrite_and_resize() {
        let fs = fs();
        fs.create_dir_all(Path::new("/a")).unwrap();
        let file = fs.create(Path::new("/a/b")).unwrap();
        file.write_all_at(b"abc", 0).unwrap();
        file.set_len(1).unwrap();
        assert_eq!(file.len().unwrap(), 1);
        let mut buf = [0u8; 1];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"a");
    }

    #[test]
    fn test_read_past_end() {
        let fs = fs();
        fs.create_dir_all(Path::new("/x")).unwrap();
        let file = fs.create(Path::new("/x/y")).unwrap();
        let mut buf = [0u8; 1];
        let err = file.read_exact_at(&mut buf, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_remove_file() {
        let fs = fs();
        fs.create_dir_all(Path::new("/d")).unwrap();
        fs.create(Path::new("/d/f")).unwrap();
        assert!(fs.exists(Path::new("/d/f")));
        fs.remove_file(Path::new("/d/f")).unwrap();
        assert!(!fs.exists(Path::new("/d/f")));
        let err = fs.remove_file(Path::new("/d/f")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_create_dir_all_and_read_dir() {
        let fs = fs();
        fs.create_dir_all(Path::new("/a/b/c")).unwrap();
        fs.create(Path::new("/a/b/c/file1")).unwrap();
        fs.create(Path::new("/a/b/c/file2")).unwrap();
        let mut entries = fs.read_dir(Path::new("/a/b/c")).unwrap();
        entries.sort();
        assert_eq!(entries, vec!["file1", "file2"]);
        let mut parent_entries = fs.read_dir(Path::new("/a/b")).unwrap();
        parent_entries.sort();
        assert_eq!(parent_entries, vec!["c"]);
    }

    #[test]
    fn test_remove_dir_all() {
        let fs = fs();
        fs.create_dir_all(Path::new("/p/q")).unwrap();
        fs.create(Path::new("/p/q/f1")).unwrap();
        fs.create(Path::new("/p/q/f2")).unwrap();
        fs.remove_dir_all(Path::new("/p")).unwrap();
        assert!(!fs.exists(Path::new("/p/q/f1")));
        assert!(!fs.exists(Path::new("/p/q")));
        assert!(!fs.exists(Path::new("/p")));
    }

    #[test]
    fn test_rename_file() {
        let fs = fs();
        fs.create_dir_all(Path::new("/r")).unwrap();
        let file = fs.create(Path::new("/r/f")).unwrap();
        file.write_all_at(b"data", 0).unwrap();
        fs.rename(Path::new("/r/f"), Path::new("/r/g")).unwrap();
        assert!(!fs.exists(Path::new("/r/f")));
        assert!(fs.exists(Path::new("/r/g")));
        let file2 = fs.open(Path::new("/r/g")).unwrap();
        let mut buf = [0u8; 4];
        file2.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"data");
    }

    #[test]
    fn test_rename_errors() {
        let fs = fs();
        fs.create_dir_all(Path::new("/d1")).unwrap();
        fs.create(Path::new("/d1/f")).unwrap();
        let err = fs
            .rename(Path::new("/d1/f"), Path::new("/d2/f"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        fs.create_dir_all(Path::new("/d2")).unwrap();
        fs.create_dir_all(Path::new("/d2/f")).unwrap();
        let err = fs
            .rename(Path::new("/d1/f"), Path::new("/d2/f"))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn test_create_dir_conflict_with_file() {
        let fs = fs();
        fs.create_dir_all(Path::new("/z")).unwrap();
        fs.create(Path::new("/z/file")).unwrap();
        let err = fs.create_dir_all(Path::new("/z/file/a")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn test_read_dir_nonexistent() {
        let fs = fs();
        let err = fs.read_dir(Path::new("/nope")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn test_exists() {
        let fs = fs();
        fs.create_dir_all(Path::new("/e")).unwrap();
        fs.create(Path::new("/e/f")).unwrap();
        assert!(fs.exists(Path::new("/e")));
        assert!(fs.exists(Path::new("/e/f")));
        assert!(!fs.exists(Path::new("/e/nope")));
    }
}
