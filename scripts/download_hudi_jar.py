#!/usr/bin/env python3
"""
Download Hudi JAR for Spark 3.5.1
"""

import urllib.request
import os
import sys

def download_hudi_jar():
    """Download Hudi JAR file."""
    # Try different Hudi versions compatible with Spark 3.5.x
    # Hudi 0.13.x supports Spark 3.3+, and 0.14.x may not have Spark 3.5 bundle yet
    # Let's try Spark 3.3 bundle which should work with Spark 3.5
    hudi_version = "0.13.1"
    scala_version = "2.12"
    spark_version = "3.3"  # Use 3.3 bundle which is compatible with 3.5
    
    jar_name = f"hudi-spark{spark_version}-bundle_{scala_version}-{hudi_version}.jar"
    url = f"https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark{spark_version}-bundle_{scala_version}/{hudi_version}/{jar_name}"
    
    # Get project root (parent of scripts directory)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    jars_dir = os.path.join(project_root, "jars")
    
    # Create jars directory if it doesn't exist
    os.makedirs(jars_dir, exist_ok=True)
    
    output_path = os.path.join(jars_dir, jar_name)
    
    print(f"Downloading Hudi JAR...")
    print(f"  URL: {url}")
    print(f"  Output: {output_path}")
    
    try:
        # Download with progress
        def show_progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            percent = min(downloaded * 100 / total_size, 100) if total_size > 0 else 0
            sys.stdout.write(f"\r  Progress: {percent:.1f}% ({downloaded:,} / {total_size:,} bytes)")
            sys.stdout.flush()
        
        urllib.request.urlretrieve(url, output_path, show_progress)
        print()  # New line after progress
        
        # Verify download
        file_size = os.path.getsize(output_path)
        print(f"✅ Downloaded successfully: {file_size:,} bytes")
        
        if file_size < 10000:  # JAR should be at least 10KB
            print(f"⚠️  WARNING: File seems too small ({file_size} bytes). May be an error page.")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error downloading: {e}")
        return False

if __name__ == "__main__":
    success = download_hudi_jar()
    sys.exit(0 if success else 1)

