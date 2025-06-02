"""
Document Margin Analyzer - S3 GUI Components
Copyright (C) 2024 Noa J Oliver
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import logging
from typing import Optional, Callable, List
import threading

from s3_handler import S3FileHandler, S3Config, S3_AVAILABLE

logger = logging.getLogger(__name__)


class S3ConfigDialog(tk.Toplevel):
    """Dialog for configuring S3 settings"""
    
    def __init__(self, parent, current_config: Optional[S3Config] = None):
        super().__init__(parent)
        self.parent = parent
        self.config = current_config or S3Config()
        self.result = None
        
        self.title("S3 Configuration")
        self.geometry("500x400")
        self.resizable(False, False)
        
        # Make dialog modal
        self.transient(parent)
        self.grab_set()
        
        self.setup_ui()
        self.load_current_config()
        
        # Center the dialog
        self.update_idletasks()
        x = (self.winfo_screenwidth() // 2) - (self.winfo_width() // 2)
        y = (self.winfo_screenheight() // 2) - (self.winfo_height() // 2)
        self.geometry(f"+{x}+{y}")
        
    def setup_ui(self):
        """Setup the dialog UI"""
        main_frame = ttk.Frame(self, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title_label = ttk.Label(main_frame, text="AWS S3 Configuration", 
                               font=('Helvetica', 12, 'bold'))
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        # AWS Profile
        ttk.Label(main_frame, text="AWS Profile:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.profile_var = tk.StringVar()
        self.profile_entry = ttk.Entry(main_frame, textvariable=self.profile_var, width=30)
        self.profile_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5)
        ttk.Label(main_frame, text="(Leave empty for 'default' profile)", 
                 foreground="gray", font=('Helvetica', 8)).grid(row=2, column=1, sticky=tk.W)
        
        # Region
        ttk.Label(main_frame, text="AWS Region:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.region_var = tk.StringVar()
        self.region_combo = ttk.Combobox(main_frame, textvariable=self.region_var, width=30)
        self.region_combo.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=5)
        self.region_combo['values'] = [
            'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
            'eu-west-1', 'eu-west-2', 'eu-central-1',
            'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1'
        ]
        
        # Max concurrent downloads
        ttk.Label(main_frame, text="Max Concurrent Downloads:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.max_downloads_var = tk.IntVar()
        self.max_downloads_spin = ttk.Spinbox(main_frame, from_=1, to=20, 
                                            textvariable=self.max_downloads_var, width=10)
        self.max_downloads_spin.grid(row=4, column=1, sticky=tk.W, pady=5)
        
        # Test connection button
        self.test_btn = ttk.Button(main_frame, text="Test Connection", 
                                  command=self.test_connection)
        self.test_btn.grid(row=5, column=0, columnspan=2, pady=20)
        
        # Status label
        self.status_label = ttk.Label(main_frame, text="", foreground="gray")
        self.status_label.grid(row=6, column=0, columnspan=2, pady=5)
        
        # Separator
        ttk.Separator(main_frame, orient='horizontal').grid(row=7, column=0, 
                                                           columnspan=2, sticky=(tk.W, tk.E), pady=10)
        
        # Info text
        info_text = ("AWS Credential Configuration:\n"
                    "This dialog configures which AWS profile and region to use.\n"
                    "Your actual AWS credentials must be set up separately using:\n\n"
                    "• AWS CLI: Run 'aws configure' in terminal\n"
                    "• Environment variables: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY\n"
                    "• Credentials file: ~/.aws/credentials (or C:\\Users\\USER\\.aws\\credentials)\n"
                    "• IAM roles: Automatic for EC2 instances")
        info_label = ttk.Label(main_frame, text=info_text, foreground="gray", 
                              justify=tk.LEFT, font=('Helvetica', 9))
        info_label.grid(row=8, column=0, columnspan=2, pady=5)
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=9, column=0, columnspan=2, pady=(20, 0))
        
        ttk.Button(button_frame, text="OK", command=self.ok_clicked).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=self.cancel_clicked).pack(side=tk.LEFT, padx=5)
        
        # Configure column weights
        main_frame.columnconfigure(1, weight=1)
        
    def load_current_config(self):
        """Load current configuration values"""
        self.profile_var.set(self.config.profile_name or '')
        self.region_var.set(self.config.region_name)
        self.max_downloads_var.set(self.config.max_concurrent_downloads)
        
    def test_connection(self):
        """Test S3 connection with current settings"""
        self.test_btn.configure(state='disabled')
        self.status_label.configure(text="Testing connection...", foreground="blue")
        self.update_idletasks()
        
        # Run test in thread to avoid blocking UI
        thread = threading.Thread(target=self._test_connection_thread)
        thread.daemon = True
        thread.start()
        
    def _test_connection_thread(self):
        """Test connection in background thread"""
        try:
            # Create temporary config
            profile = self.profile_var.get().strip()
            test_config = S3Config(
                profile_name=profile if profile else None,
                region_name=self.region_var.get(),
                max_concurrent_downloads=self.max_downloads_var.get()
            )
            
            # Test connection
            handler = S3FileHandler(test_config)
            success, error = handler.verify_credentials()
            
            if success:
                # Try to list buckets
                buckets = handler.get_bucket_list()
                message = f"Connection successful! Found {len(buckets)} bucket(s)"
                color = "green"
            else:
                message = f"Connection failed: {error}"
                color = "red"
                
        except Exception as e:
            message = f"Error: {str(e)}"
            color = "red"
            
        # Update UI in main thread
        self.after(0, self._update_test_status, message, color)
        
    def _update_test_status(self, message, color):
        """Update test status in main thread"""
        self.status_label.configure(text=message, foreground=color)
        self.test_btn.configure(state='normal')
        
    def ok_clicked(self):
        """Handle OK button click"""
        # Create new config
        profile = self.profile_var.get().strip()
        self.result = S3Config(
            profile_name=profile if profile else None,
            region_name=self.region_var.get(),
            max_concurrent_downloads=self.max_downloads_var.get()
        )
        self.destroy()
        
    def cancel_clicked(self):
        """Handle Cancel button click"""
        self.result = None
        self.destroy()


class S3BrowserDialog(tk.Toplevel):
    """Dialog for browsing S3 buckets and folders"""
    
    def __init__(self, parent, s3_handler: S3FileHandler, initial_path: str = ""):
        super().__init__(parent)
        self.parent = parent
        self.s3_handler = s3_handler
        self.result = None
        
        self.title("Browse S3")
        self.geometry("700x500")
        
        # Make dialog modal
        self.transient(parent)
        self.grab_set()
        
        self.setup_ui()
        
        # Load initial content
        if initial_path:
            self.path_var.set(initial_path)
        self.refresh_listing()
        
        # Center the dialog
        self.update_idletasks()
        x = (self.winfo_screenwidth() // 2) - (self.winfo_width() // 2)
        y = (self.winfo_screenheight() // 2) - (self.winfo_height() // 2)
        self.geometry(f"+{x}+{y}")
        
    def setup_ui(self):
        """Setup the browser UI"""
        main_frame = ttk.Frame(self, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # Path entry
        path_frame = ttk.Frame(main_frame)
        path_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        path_frame.columnconfigure(1, weight=1)
        
        ttk.Label(path_frame, text="S3 Path:").grid(row=0, column=0, padx=(0, 5))
        self.path_var = tk.StringVar(value="s3://")
        self.path_entry = ttk.Entry(path_frame, textvariable=self.path_var)
        self.path_entry.grid(row=0, column=1, sticky=(tk.W, tk.E))
        self.path_entry.bind('<Return>', lambda e: self.refresh_listing())
        
        ttk.Button(path_frame, text="Go", command=self.refresh_listing).grid(row=0, column=2, padx=(5, 0))
        
        # Bucket selection
        bucket_frame = ttk.Frame(main_frame)
        bucket_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        bucket_frame.columnconfigure(1, weight=1)
        
        ttk.Label(bucket_frame, text="Bucket:").grid(row=0, column=0, padx=(0, 5))
        self.bucket_var = tk.StringVar()
        self.bucket_combo = ttk.Combobox(bucket_frame, textvariable=self.bucket_var)
        self.bucket_combo.grid(row=0, column=1, sticky=(tk.W, tk.E))
        self.bucket_combo.bind('<<ComboboxSelected>>', self.bucket_selected)
        
        # Load buckets
        self.load_buckets()
        
        # Treeview for browsing
        tree_frame = ttk.Frame(main_frame)
        tree_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        
        # Create treeview with scrollbars
        self.tree = ttk.Treeview(tree_frame, columns=('size', 'modified'), show='tree headings')
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure columns
        self.tree.heading('#0', text='Name')
        self.tree.heading('size', text='Size')
        self.tree.heading('modified', text='Modified')
        
        self.tree.column('#0', width=400)
        self.tree.column('size', width=100)
        self.tree.column('modified', width=150)
        
        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.tree.configure(yscrollcommand=vsb.set)
        
        hsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.tree.configure(xscrollcommand=hsb.set)
        
        # Double-click to navigate
        self.tree.bind('<Double-Button-1>', self.item_double_clicked)
        
        # Status label
        self.status_label = ttk.Label(main_frame, text="", foreground="gray")
        self.status_label.grid(row=3, column=0, pady=10)
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=4, column=0)
        
        ttk.Button(button_frame, text="Select Folder", 
                  command=self.select_clicked).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", 
                  command=self.cancel_clicked).pack(side=tk.LEFT, padx=5)
        
    def load_buckets(self):
        """Load list of S3 buckets"""
        try:
            buckets = self.s3_handler.get_bucket_list()
            self.bucket_combo['values'] = buckets
            
            # Select first bucket if available
            if buckets and not self.bucket_var.get():
                self.bucket_var.set(buckets[0])
                
        except Exception as e:
            logger.error(f"Error loading buckets: {e}")
            self.status_label.configure(text=f"Error: {str(e)}", foreground="red")
            
    def bucket_selected(self, event=None):
        """Handle bucket selection"""
        bucket = self.bucket_var.get()
        if bucket:
            self.path_var.set(f"s3://{bucket}/")
            self.refresh_listing()
            
    def refresh_listing(self):
        """Refresh the S3 listing"""
        # Clear current items
        for item in self.tree.get_children():
            self.tree.delete(item)
            
        path = self.path_var.get()
        if not path or not path.startswith('s3://'):
            return
            
        self.status_label.configure(text="Loading...", foreground="blue")
        self.update_idletasks()
        
        # Run in thread
        thread = threading.Thread(target=self._load_listing_thread, args=(path,))
        thread.daemon = True
        thread.start()
        
    def _load_listing_thread(self, path):
        """Load S3 listing in background thread"""
        try:
            # Parse bucket from path
            if path.startswith('s3://'):
                parts = path[5:].split('/', 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ''
                
                # Update bucket combo if needed
                if bucket != self.bucket_var.get():
                    self.after(0, self.bucket_var.set, bucket)
            
            # List objects
            objects = self.s3_handler.list_objects(path, recursive=False)
            
            # Group by folders and files
            folders = set()
            files = []
            
            for obj in objects:
                key = obj['Key']
                if prefix:
                    key = key[len(prefix):]
                    
                if '/' in key:
                    # It's in a subfolder
                    folder = key.split('/')[0]
                    folders.add(folder)
                else:
                    # It's a file in current level
                    files.append(obj)
                    
            # Update UI in main thread
            self.after(0, self._update_listing, sorted(folders), files)
            self.after(0, self.status_label.configure, 
                      text=f"Found {len(folders)} folders, {len(files)} files", 
                      foreground="green")
            
        except Exception as e:
            logger.error(f"Error listing S3 objects: {e}")
            self.after(0, self.status_label.configure, 
                      text=f"Error: {str(e)}", foreground="red")
            
    def _update_listing(self, folders, files):
        """Update treeview with listing results"""
        # Add folders
        for folder in folders:
            self.tree.insert('', 'end', text=f"📁 {folder}/", values=('', ''))
            
        # Add files
        for file_obj in files:
            name = os.path.basename(file_obj['Key'])
            size = self._format_size(file_obj['Size'])
            modified = file_obj['LastModified'].strftime('%Y-%m-%d %H:%M')
            self.tree.insert('', 'end', text=f"📄 {name}", values=(size, modified))
            
    def _format_size(self, size):
        """Format file size for display"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
        
    def item_double_clicked(self, event):
        """Handle double-click on tree item"""
        selection = self.tree.selection()
        if not selection:
            return
            
        item = self.tree.item(selection[0])
        text = item['text']
        
        if text.startswith('📁'):
            # It's a folder - navigate into it
            folder = text[2:].rstrip('/')
            current_path = self.path_var.get()
            if not current_path.endswith('/'):
                current_path += '/'
            self.path_var.set(current_path + folder + '/')
            self.refresh_listing()
            
    def select_clicked(self):
        """Handle select button click"""
        self.result = self.path_var.get()
        self.destroy()
        
    def cancel_clicked(self):
        """Handle cancel button click"""
        self.result = None
        self.destroy()
        

def check_s3_available():
    """Check if S3 support is available"""
    return S3_AVAILABLE