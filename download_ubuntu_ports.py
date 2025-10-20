#!/usr/bin/env python3
"""
Script to download all files from Ubuntu Ports repository subdirectories.
Downloads files from https://ports.ubuntu.com/ubuntu-ports/pool/universe
"""

import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
from pathlib import Path
import logging
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class UbuntuPortsDownloader:
    def __init__(self, base_url, output_dir="downloads", start_dir=None, end_dir=None):
        self.base_url = base_url.rstrip('/')
        self.output_dir = output_dir
        self.start_dir = start_dir  # First-level directory to start from (e.g., 'h/')
        self.end_dir = end_dir      # First-level directory to end at (e.g., 'm/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.downloaded_count = 0
        self.failed_count = 0
        self.skipped_count = 0
        
    def get_directory_listing(self, url):
        """Fetch and parse directory listing from URL."""
        try:
            logger.info(f"Fetching directory: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            links = []
            
            # Find all links in the page
            for link in soup.find_all('a'):
                href = link.get('href')
                if href and href not in ['../', '../', '/', '#']:
                    # Skip parent directory and root links
                    if not href.startswith('http') and not href.startswith('/'):
                        links.append(href)
            
            return links
        except Exception as e:
            logger.error(f"Error fetching directory {url}: {e}")
            return []
    
    def is_directory(self, link):
        """Check if a link is a directory (ends with /)."""
        return link.endswith('/')
    
    def should_process_directory(self, dirname):
        """Check if a first-level directory should be processed based on start_dir and end_dir."""
        # Remove trailing slash for comparison
        dirname = dirname.rstrip('/')
        
        # If no filters set, process all directories
        if not self.start_dir and not self.end_dir:
            return True
        
        # If only start_dir is set, process from start_dir onwards
        if self.start_dir and not self.end_dir:
            return dirname >= self.start_dir.rstrip('/')
        
        # If only end_dir is set, process up to end_dir
        if not self.start_dir and self.end_dir:
            return dirname <= self.end_dir.rstrip('/')
        
        # If both are set, process the range
        start = self.start_dir.rstrip('/')
        end = self.end_dir.rstrip('/')
        return start <= dirname <= end
    
    def download_file(self, url, local_path):
        """Download a file from URL to local path."""
        try:
            # Check if file already exists
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                # Get remote file size
                head_response = self.session.head(url, timeout=10)
                remote_size = int(head_response.headers.get('content-length', 0))
                
                if local_size == remote_size:
                    logger.info(f"Skipping (already exists): {os.path.basename(local_path)}")
                    self.skipped_count += 1
                    return True
            
            logger.info(f"Downloading: {url}")
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # Get file size for progress
            total_size = int(response.headers.get('content-length', 0))
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download with progress
            downloaded = 0
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            size_mb = downloaded / (1024 * 1024)
            logger.info(f"✓ Downloaded: {os.path.basename(local_path)} ({size_mb:.2f} MB)")
            self.downloaded_count += 1
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to download {url}: {e}")
            self.failed_count += 1
            return False
    
    def get_local_path(self, url):
        """Convert URL to local file path."""
        parsed = urlparse(url)
        # Remove the base URL part to get relative path
        relative_path = parsed.path
        if relative_path.startswith('/ubuntu-ports/pool/universe/'):
            relative_path = relative_path.replace('/ubuntu-ports/pool/universe/', '', 1)
        
        local_path = os.path.join(self.output_dir, relative_path)
        return local_path
    
    def crawl_and_download(self, url, depth=0):
        """Recursively crawl directories and download files."""
        if depth > 20:  # Prevent infinite recursion
            logger.warning(f"Maximum depth reached for {url}")
            return
        
        indent = "  " * depth
        logger.info(f"{indent}Exploring: {url}")
        
        links = self.get_directory_listing(url)
        
        if not links:
            logger.warning(f"{indent}No links found in {url}")
            return
        
        for link in links:
            full_url = urljoin(url + '/', link)
            
            if self.is_directory(link):
                # At first level (depth=0), check if directory should be processed
                if depth == 0 and not self.should_process_directory(link):
                    logger.info(f"{indent}Skipping directory (filtered): {link}")
                    continue
                
                # Recursively crawl subdirectory
                self.crawl_and_download(full_url, depth + 1)
            else:
                # Download file
                local_path = self.get_local_path(full_url)
                self.download_file(full_url, local_path)
                
                # Small delay to be nice to the server
                time.sleep(0.1)
    
    def start(self):
        """Start the download process."""
        logger.info("="*70)
        logger.info("Ubuntu Ports Downloader")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Output Directory: {self.output_dir}")
        if self.start_dir:
            logger.info(f"Start Directory: {self.start_dir}")
        if self.end_dir:
            logger.info(f"End Directory: {self.end_dir}")
        logger.info("="*70)
        
        start_time = time.time()
        
        try:
            self.crawl_and_download(self.base_url)
        except KeyboardInterrupt:
            logger.info("\n\nDownload interrupted by user")
        
        elapsed = time.time() - start_time
        
        logger.info("="*70)
        logger.info("Download Summary")
        logger.info(f"Downloaded: {self.downloaded_count} files")
        logger.info(f"Skipped: {self.skipped_count} files")
        logger.info(f"Failed: {self.failed_count} files")
        logger.info(f"Time elapsed: {elapsed:.2f} seconds")
        logger.info("="*70)


def main():
    parser = argparse.ArgumentParser(
        description='Download files from Ubuntu Ports repository with optional directory filtering.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Download all files from default URL
  python download_ubuntu_ports.py

  # Download from a specific URL
  python download_ubuntu_ports.py --url https://ports.ubuntu.com/ubuntu-ports/pool/main

  # Download to a custom directory
  python download_ubuntu_ports.py --output my_downloads

  # Download from directory 'h' onwards
  python download_ubuntu_ports.py --start-dir h

  # Download up to directory 'm'
  python download_ubuntu_ports.py --end-dir m

  # Download from 'h' to 'm' range
  python download_ubuntu_ports.py --start-dir h --end-dir m

  # Complete example with all options
  python download_ubuntu_ports.py --url https://ports.ubuntu.com/ubuntu-ports/pool/universe --output downloads --start-dir h --end-dir m
        '''
    )
    
    parser.add_argument(
        '--url',
        type=str,
        default='https://ports.ubuntu.com/ubuntu-ports/pool/universe',
        help='Base URL to download from (default: %(default)s)'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        type=str,
        default='ubuntu_ports_downloads',
        help='Output directory to save downloaded files (default: %(default)s)'
    )
    
    parser.add_argument(
        '--start-dir',
        type=str,
        default=None,
        help='First-level directory to start from (e.g., "h" to start from h/). Useful for resuming or splitting downloads.'
    )
    
    parser.add_argument(
        '--end-dir',
        type=str,
        default=None,
        help='First-level directory to end at (e.g., "m" to stop at m/). Useful for downloading specific ranges.'
    )
    
    args = parser.parse_args()
    
    # Log configuration
    logger.info(f"Starting download from: {args.url}")
    logger.info(f"Files will be saved to: {args.output}")
    if args.start_dir:
        logger.info(f"Starting from directory: {args.start_dir}")
    if args.end_dir:
        logger.info(f"Ending at directory: {args.end_dir}")
    
    downloader = UbuntuPortsDownloader(args.url, args.output, args.start_dir, args.end_dir)
    downloader.start()


if __name__ == "__main__":
    main()

