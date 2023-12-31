use crate::bitcask::{FileId, Key, Value};
use crate::error::BitCaskError;
use crate::log_entry::DiskLogEntry;
use crate::log_file::DiskLogFile;
use crate::memory_index::{MemIndex, MemIndexEntry};
use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use tracing::trace;

pub(crate) struct DiskLog {
    files: Vec<DiskLogFile>,
    data_dir: PathBuf,
    current_file_size: u64,
    immutable: bool,
}

impl DiskLog {
    /// Initialize the disk log from the immutable files. This method is called when compaction is started.
    pub(crate) fn immutable_initialization(
        immutable_files: Vec<PathBuf>,
        mem_index: &mut MemIndex,
    ) -> Result<Self, BitCaskError> {
        let files = Self::to_disk_log_files(immutable_files, mem_index)?;
        let data_dir = files.first().unwrap().path.parent().unwrap().to_path_buf();

        Ok(Self {
            files,
            data_dir,
            current_file_size: 0,
            immutable: true,
        })
    }

    /// create a new log file with file id 0.
    fn new<T: Into<PathBuf> + Clone>(data_dir: T) -> Result<Self, BitCaskError> {
        let data_dir_path_buf: PathBuf = data_dir.clone().into();
        Ok(Self {
            files: vec![DiskLogFile::new(data_dir, 0)?],
            data_dir: data_dir_path_buf,
            current_file_size: 0,
            immutable: false,
        })
    }

    /// If the data directory is empty, create a new log file with file id 0.
    /// Otherwise, load all the log files from disk and populate the memory index.
    pub(crate) fn from_disk<T: Into<PathBuf>>(
        data_dir: T,
        mem_index: &mut MemIndex,
    ) -> Result<Self, BitCaskError> {
        let data_dir: PathBuf = data_dir.into();

        let files = std::fs::read_dir(&data_dir)?
            .filter_map(|path| {
                path.ok().map(|path| path.path()).filter(|path| {
                    path.is_file() && path.extension() == Some(OsStr::new(DiskLogFile::EXT))
                })
            })
            .collect();
        let files = Self::to_disk_log_files(files, mem_index)?;

        if (files.len()) == 0 {
            trace!("No disk log files found, starting from scratch");
            return Self::new(data_dir);
        }

        let current_file_size = files.last().unwrap().file.metadata()?.len();

        Ok(Self {
            files,
            data_dir,
            current_file_size,
            immutable: false,
        })
    }

    fn current_file(&mut self) -> (&mut DiskLogFile, FileId) {
        // the last file is always open for appending
        let disk_log_file = self.files.last_mut().unwrap();
        let file_id = disk_log_file.file_id;
        (disk_log_file, file_id)
    }

    fn get_file(&self, file_id: FileId) -> &DiskLogFile {
        self.files.get(file_id as usize).unwrap()
    }

    pub(crate) fn get(&self, mem_index_entry: &MemIndexEntry) -> Result<Value, BitCaskError> {
        let MemIndexEntry {
            value_offset,
            value_size,
            file_id,
        } = mem_index_entry;
        if *value_size == 0 {
            return Err(BitCaskError::ValueNotFound);
        }
        let disk_log_file = self.get_file(*file_id);
        let mut buffered_reader =
            BufReader::with_capacity(*value_size as usize, &disk_log_file.file);
        buffered_reader.seek(SeekFrom::Start(*value_offset))?;
        let mut buf = vec![0u8; *value_size as usize];
        buffered_reader.read_exact(buf.as_mut())?;
        Ok(Value::from(buf))
    }

    pub(crate) fn put(&mut self, key: &Key, value: &Value) -> Result<MemIndexEntry, BitCaskError> {
        self.append(DiskLogEntry::new_entry(key.clone(), value.clone()))
    }

    pub(crate) fn delete(&mut self, key: &Key) -> Result<MemIndexEntry, BitCaskError> {
        self.append(DiskLogEntry::new_tombstone(key.clone()))
    }

    fn append(&mut self, entry: DiskLogEntry) -> Result<MemIndexEntry, BitCaskError> {
        if self.immutable {
            panic!("Cannot append to an immutable disk log");
        }
        let (disk_log_file, file_id) = self.current_file();
        let value_offset = disk_log_file.append_new_entry(entry.clone())?;
        self.current_file_size += entry.total_byte_size();
        // check if the current file exceeds the max file size, if so, create a new file
        if self.current_file_size > DiskLogFile::MAX_FILE_SIZE {
            self.check_file_size()?;
        }
        Ok(MemIndexEntry {
            file_id,
            value_offset,
            value_size: entry.value_byte_size(),
        })
    }

    fn check_file_size(&mut self) -> Result<(), BitCaskError> {
        let (disk_log_file, file_id) = self.current_file();
        let file = &mut disk_log_file.file;
        let file_size = file.metadata()?.len();
        if file_size > DiskLogFile::MAX_FILE_SIZE {
            trace!(
                "Disk log file {} exceeds max file size, creating a new file",
                file_id
            );
            self.create_new_file()?;
        }
        Ok(())
    }

    pub(crate) fn get_immutable_files(&self) -> Vec<PathBuf> {
        let last_file_id = self.files.last().unwrap().file_id;
        self.files
            .iter()
            .filter(|disk_log_file| disk_log_file.file_id != last_file_id)
            .map(|disk_log_file| disk_log_file.path.clone())
            .collect()
    }

    /// Invoked when the user calls `compact_to_new_dir` or library call `check_file_size`.
    pub(crate) fn create_new_file(&mut self) -> Result<(), BitCaskError> {
        let last_file_id = self.files.last().unwrap().file_id;
        let new_file_id = last_file_id + 1;
        let new_file = DiskLogFile::new(&self.data_dir, new_file_id)?;
        self.files.push(new_file);
        Ok(())
    }

    pub(crate) fn copy_files_to_new_dir(
        &self,
        immutable_files: Vec<PathBuf>,
        new_log_file_path: PathBuf,
    ) -> Result<(), BitCaskError> {
        // exclude the immutable files from the self.files
        let mut files: Vec<PathBuf> = self
            .files
            .iter()
            .filter(|disk_log_file| !immutable_files.contains(&disk_log_file.path))
            .map(|disk_log_file| disk_log_file.path.clone())
            .collect();
        // copy the files to the new directory
        for file in files.iter_mut() {
            let mut new_file = new_log_file_path.clone();
            new_file.push(file.file_name().unwrap());
            std::fs::copy(file, new_file)?;
        }
        Ok(())
    }

    pub(crate) fn to_disk_log_files(
        files: Vec<PathBuf>,
        mem_index: &mut MemIndex,
    ) -> Result<Vec<DiskLogFile>, BitCaskError> {
        let mut files = files
            .into_iter()
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|file_stem| file_stem.to_str())
                    .and_then(|file_stem| file_stem.parse::<FileId>().ok())
                    .map(|file_id| (file_id, path))
            })
            .map(|(file_id, path)| {
                DiskLogFile::open(file_id, path, mem_index)
                    .map(|disk_log_file| (file_id, disk_log_file))
            })
            .collect::<Result<Vec<(FileId, DiskLogFile)>, BitCaskError>>()?;

        files.sort_by_key(|(file_id, _)| *file_id);
        Ok(files
            .into_iter()
            .map(|(_, disk_log_file)| disk_log_file)
            .collect())
    }
}
