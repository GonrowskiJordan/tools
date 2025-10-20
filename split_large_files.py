#!/usr/bin/env python3
"""
Script to scan all files in subdirectories, split files exceeding 90MB,
and remove the original files after splitting.
"""

import os
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FileSplitter:
    def __init__(self, base_directory, max_size_mb=90, chunk_size_mb=85):
        """
        Initialize the file splitter.
        
        Args:
            base_directory: The root directory to scan
            max_size_mb: Maximum file size in MB before splitting (default: 90MB)
            chunk_size_mb: Size of each chunk in MB (default: 85MB)
        """
        self.base_directory = base_directory
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.chunk_size_bytes = chunk_size_mb * 1024 * 1024
        self.files_scanned = 0
        self.files_split = 0
        self.files_failed = 0
        
    def get_file_size_mb(self, file_path):
        """Get file size in MB."""
        return os.path.getsize(file_path) / (1024 * 1024)
    
    def split_file(self, file_path):
        """
        Split a file into chunks and remove the original.
        
        Args:
            file_path: Path to the file to split
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            logger.info(f"Splitting file: {file_path} ({file_size_mb:.2f} MB)")
            
            # Calculate number of chunks needed
            num_chunks = (file_size + self.chunk_size_bytes - 1) // self.chunk_size_bytes
            
            # Read and split the file
            with open(file_path, 'rb') as source_file:
                for chunk_num in range(1, num_chunks + 1):
                    # Create chunk filename
                    chunk_filename = f"{file_path}.part{chunk_num:03d}"
                    
                    # Read chunk data
                    chunk_data = source_file.read(self.chunk_size_bytes)
                    
                    # Write chunk to disk
                    with open(chunk_filename, 'wb') as chunk_file:
                        chunk_file.write(chunk_data)
                    
                    chunk_size_mb = len(chunk_data) / (1024 * 1024)
                    logger.info(f"  ✓ Created: {os.path.basename(chunk_filename)} ({chunk_size_mb:.2f} MB)")
            
            # Remove original file
            os.remove(file_path)
            logger.info(f"  ✓ Removed original file: {os.path.basename(file_path)}")
            logger.info(f"  → Split into {num_chunks} chunks")
            
            self.files_split += 1
            return True
            
        except Exception as e:
            logger.error(f"  ✗ Failed to split {file_path}: {e}")
            self.files_failed += 1
            return False
    
    def scan_and_split(self):
        """Scan all files in the base directory and split files exceeding max size."""
        logger.info("="*70)
        logger.info("File Splitter")
        logger.info(f"Base Directory: {self.base_directory}")
        logger.info(f"Max File Size: {self.max_size_bytes / (1024 * 1024):.0f} MB")
        logger.info(f"Chunk Size: {self.chunk_size_bytes / (1024 * 1024):.0f} MB")
        logger.info("="*70)
        
        if not os.path.exists(self.base_directory):
            logger.error(f"Directory does not exist: {self.base_directory}")
            return
        
        # Walk through all subdirectories
        for root, dirs, files in os.walk(self.base_directory):
            for filename in files:
                # Skip files that are already part files
                if '.part' in filename:
                    continue
                
                file_path = os.path.join(root, filename)
                self.files_scanned += 1
                
                try:
                    file_size = os.path.getsize(file_path)
                    
                    # Check if file exceeds max size
                    if file_size > self.max_size_bytes:
                        file_size_mb = file_size / (1024 * 1024)
                        logger.info(f"\nFound large file: {file_path} ({file_size_mb:.2f} MB)")
                        self.split_file(file_path)
                    
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    self.files_failed += 1
        
        # Print summary
        logger.info("\n" + "="*70)
        logger.info("Split Summary")
        logger.info(f"Files scanned: {self.files_scanned}")
        logger.info(f"Files split: {self.files_split}")
        logger.info(f"Files failed: {self.files_failed}")
        logger.info("="*70)


def main():
    """Main function."""
    # Default configuration
    BASE_DIRECTORY = "ubuntu_ports_downloads"
    MAX_SIZE_MB = 90
    CHUNK_SIZE_MB = 85
    
    # Accept command line arguments
    if len(sys.argv) > 1:
        BASE_DIRECTORY = sys.argv[1]
    
    if len(sys.argv) > 2:
        try:
            MAX_SIZE_MB = int(sys.argv[2])
        except ValueError:
            logger.error(f"Invalid max size: {sys.argv[2]}")
            sys.exit(1)
    
    if len(sys.argv) > 3:
        try:
            CHUNK_SIZE_MB = int(sys.argv[3])
        except ValueError:
            logger.error(f"Invalid chunk size: {sys.argv[3]}")
            sys.exit(1)
    
    # Validate chunk size is less than max size
    if CHUNK_SIZE_MB >= MAX_SIZE_MB:
        logger.error(f"Chunk size ({CHUNK_SIZE_MB} MB) must be less than max size ({MAX_SIZE_MB} MB)")
        sys.exit(1)
    
    # Create splitter and run
    splitter = FileSplitter(BASE_DIRECTORY, MAX_SIZE_MB, CHUNK_SIZE_MB)
    
    try:
        splitter.scan_and_split()
    except KeyboardInterrupt:
        logger.info("\n\nOperation interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()

