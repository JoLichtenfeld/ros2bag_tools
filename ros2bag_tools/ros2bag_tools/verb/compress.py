# Copyright 2025 AIT Austrian Institute of Technology GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess
import tempfile
import yaml
import glob
from pathlib import Path

from ros2bag.verb import VerbExtension
from ros2bag.api import print_error


class CompressVerb(VerbExtension):
    """Compress ROS2 bags using ros2 bag convert with compression options."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'input_bags',
            nargs='+',
            help='Path(s) to input ROS2 bag directory/directories (supports glob patterns)'
        )
        parser.add_argument(
            '-o', '--output', 
            default=None,
            help='Output bag name (only for single input bag, default: input_bag_compressed)'
        )
        parser.add_argument(
            '-m', '--compression-mode',
            choices=['message', 'file'],
            default='message',
            help='Compression mode: message or file (default: message)'
        )
        parser.add_argument(
            '-f', '--compression-format',
            default='zstd',
            help='Compression format: zstd, lz4, etc. (default: zstd)'
        )
        parser.add_argument(
            '--max-size',
            type=int,
            default=1000000000,
            help='Maximum bagfile size in bytes (default: 1000000000)'
        )
        parser.add_argument(
            '--queue-size',
            type=int,
            default=0,
            help='Compression queue size for message mode (default: 0)'
        )
        parser.add_argument(
            '--validate',
            action='store_true',
            default=True,
            help='Validate message count after compression (default: True)'
        )
        parser.add_argument(
            '--no-validate',
            dest='validate',
            action='store_false',
            help='Skip message count validation'
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Print the YAML configuration used for compression'
        )

    def main(self, *, args):
        # Expand glob patterns and validate inputs
        input_bags = []
        for pattern in args.input_bags:
            # Strip trailing slashes for proper naming
            pattern = pattern.rstrip('/')
            
            # Expand glob pattern
            matches = glob.glob(pattern)
            if matches:
                # Filter to only valid bag directories
                for match in matches:
                    if self._is_valid_bag(match):
                        input_bags.append(match)
                    else:
                        print(f"Warning: Skipping '{match}' - not a valid ROS2 bag")
            else:
                # No glob matches, check if it's a direct path
                if self._is_valid_bag(pattern):
                    input_bags.append(pattern)
                elif os.path.exists(pattern):
                    print_error(f"'{pattern}' exists but is not a valid ROS2 bag")
                else:
                    print_error(f"Input bag '{pattern}' not found")

        if not input_bags:
            print_error("No valid bag files found")
            return 1

        # Check output argument constraints
        if args.output and len(input_bags) > 1:
            print_error("Cannot specify output name (-o) when compressing multiple bags")
            return 1

        print(f"Found {len(input_bags)} valid bag(s) to compress")
        
        success_count = 0
        total_count = len(input_bags)
        
        for input_bag in input_bags:
            output_bag = args.output if args.output else f"{input_bag}_compressed"
            
            # Get original bag info if validation is requested
            original_info = None
            original_messages = None
            if args.validate:
                try:
                    result = subprocess.run(
                        ['ros2', 'bag', 'info', input_bag],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    original_info = result.stdout
                    # Extract message count
                    for line in original_info.split('\n'):
                        if 'Messages:' in line:
                            original_messages = int(line.split(':')[1].strip())
                            break
                except (subprocess.CalledProcessError, ValueError):
                    print(f"Warning: Could not get original message count for {input_bag}")

            # Compress the bag
            if self._compress_single_bag(input_bag, output_bag, args, original_info, original_messages):
                success_count += 1
            else:
                print_error(f"Failed to compress: {input_bag}")

        # Summary
        if total_count > 1:
            print(f"\nCompression summary: {success_count}/{total_count} bags compressed successfully")
        
        return 0 if success_count == total_count else 1

    def _is_valid_bag(self, bag_path):
        """Check if a directory is a valid ROS2 bag."""
        if not os.path.isdir(bag_path):
            return False
            
        # Check for metadata.yaml
        metadata_path = os.path.join(bag_path, 'metadata.yaml')
        if not os.path.exists(metadata_path):
            return False
            
        # Check for at least one data file (mcap or db3)
        data_files = glob.glob(os.path.join(bag_path, '*.mcap')) + \
                    glob.glob(os.path.join(bag_path, '*.db3'))
        return len(data_files) > 0

    def _compress_single_bag(self, input_bag, output_bag, args, original_info=None, original_messages=None):
        """Compress a single bag file."""
        # Create temporary YAML configuration
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
            config = {
                'output_bags': [{
                    'uri': output_bag,
                    'max_bagfile_size': args.max_size,
                    'all_topics': True,
                    'compression_mode': args.compression_mode,
                    'compression_format': args.compression_format
                }]
            }
            
            # Add compression_queue_size only for message mode
            if args.compression_mode == 'message':
                config['output_bags'][0]['compression_queue_size'] = args.queue_size
                
            yaml.dump(config, temp_file, default_flow_style=False)
            temp_yaml_path = temp_file.name

        # Print YAML configuration if verbose mode is enabled
        if args.verbose:
            print(f"\nYAML Configuration for {input_bag}:")
            print("=" * 50)
            print(yaml.dump(config, default_flow_style=False))
            print("=" * 50)

        try:
            # Display compression info
            queue_info = f", queue={args.queue_size}" if args.compression_mode == 'message' else ""
            print(f"Compressing: {input_bag} -> {output_bag} "
                  f"({args.compression_mode}/{args.compression_format}{queue_info})")

            # Run ros2 bag convert
            result = subprocess.run(
                ['ros2', 'bag', 'convert', '-i', input_bag, '-o', temp_yaml_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print_error(f"Compression failed for {input_bag}!")
                print_error(result.stderr)
                return False

            # Validate output exists
            if not (os.path.exists(output_bag) and (os.path.isdir(output_bag) or os.path.isfile(output_bag))):
                print_error(f"Output bag not found after compression: {output_bag}")
                return False

            # Check if the output bag has valid metadata
            metadata_path = os.path.join(output_bag, 'metadata.yaml')
            if os.path.exists(metadata_path):
                try:
                    with open(metadata_path, 'r') as f:
                        content = f.read().strip()
                        if not content:
                            print_error(f"Output bag has empty metadata.yaml - compression may have failed: {output_bag}")
                            print("💡 Try using file compression mode (-m file) or adjusting queue size")
                            return False
                except Exception as e:
                    print_error(f"Could not read metadata.yaml for {output_bag}: {e}")
                    return False

            # Validate message count if requested
            if args.validate and original_messages is not None:
                try:
                    result = subprocess.run(
                        ['ros2', 'bag', 'info', output_bag],
                        capture_output=True,
                        text=True,
                        check=False  # Don't raise exception, handle error manually
                    )
                    
                    if result.returncode != 0:
                        print_error(f"Output bag appears to be corrupted - cannot read bag info: {output_bag}")
                        print_error(result.stderr)
                        print("💡 Try using file compression mode (-m file) or adjusting queue size")
                        return False
                        
                    compressed_info = result.stdout
                    compressed_messages = None
                    
                    # Extract message count
                    for line in compressed_info.split('\n'):
                        if 'Messages:' in line:
                            compressed_messages = int(line.split(':')[1].strip())
                            break
                    
                    if compressed_messages is not None:
                        if original_messages == compressed_messages:
                            # Try to calculate compression ratio
                            try:
                                original_size = self._extract_bag_size(original_info)
                                compressed_size = self._extract_bag_size(compressed_info)
                                
                                if original_size and compressed_size:
                                    ratio = (compressed_size / original_size) * 100
                                    original_size_str = self._format_size(original_size)
                                    compressed_size_str = self._format_size(compressed_size)
                                    print(f"✅ Success: {original_messages} messages preserved | "
                                          f"{original_size_str} -> {compressed_size_str} ({ratio:.1f}%)")
                                else:
                                    print(f"✅ Success: {original_messages} messages preserved")
                            except:
                                print(f"✅ Success: {original_messages} messages preserved")
                        else:
                            print_error(f"Message count mismatch! Original: {original_messages}, "
                                      f"Compressed: {compressed_messages}")
                            print("💡 Try increasing compression_queue_size or using file compression mode")
                            return False
                    else:
                        print(f"✅ Compression completed (could not validate message count)")
                        
                except subprocess.CalledProcessError:
                    print(f"✅ Compression completed (could not validate message count)")
            else:
                print(f"✅ Compression completed")

            return True

        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_yaml_path)
            except OSError:
                pass

    def _extract_bag_size(self, bag_info):
        """Extract bag size in bytes from ros2 bag info output."""
        try:
            for line in bag_info.split('\n'):
                if 'Bag size:' in line:
                    parts = line.split()
                    if len(parts) >= 4:
                        size_value = float(parts[2])
                        unit = parts[3]
                        
                        if unit == 'GiB':
                            return int(size_value * 1024 * 1024 * 1024)
                        elif unit == 'MiB':
                            return int(size_value * 1024 * 1024)
                        elif unit == 'KiB':
                            return int(size_value * 1024)
                        else:  # bytes
                            return int(size_value)
        except (ValueError, IndexError):
            pass
        return None

    def _format_size(self, size_bytes):
        """Format size in bytes to human readable format."""
        if size_bytes >= 1024**3:
            return f"{size_bytes / (1024**3):.1f} GiB"
        elif size_bytes >= 1024**2:
            return f"{size_bytes / (1024**2):.1f} MiB"
        elif size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KiB"
        else:
            return f"{size_bytes} bytes"