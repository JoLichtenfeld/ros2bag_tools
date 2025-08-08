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
import shutil
import yaml
from datetime import datetime
from typing import List, Tuple

from ros2bag.verb import VerbExtension
from ros2bag.api import print_error

from rosbag2_py import (
    SequentialReader,
    SequentialWriter,
    StorageOptions,
    ConverterOptions,
)


class OverlapVerb(VerbExtension):
    """Find temporal overlap between ROS bag files and optionally crop them."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'bags',
            nargs='+',
            help='Paths to bag files or directories containing bag files'
        )
        parser.add_argument(
            '--crop',
            action='store_true',
            help='Crop bags to overlap period'
        )
        parser.add_argument(
            '--output-dir',
            default='cropped_bags',
            help='Output directory for cropped bags (default: cropped_bags)'
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Overwrite existing output directory'
        )
        parser.add_argument(
            '--plot',
            action='store_true',
            help='Plot the time ranges of the bag files'
        )

    def main(self, *, args):
        try:
            if args.crop:
                return self._crop_bags(args)
            else:
                return self._find_overlap(args)
        except Exception as e:
            print_error(f"Error: {str(e)}")
            return 1

    def _find_overlap(self, args) -> int:
        """Find the temporal overlap between multiple bag files."""
        # Get all bag files from the specified paths
        bag_files = self._get_all_bag_files(args.bags)
        
        if not bag_files:
            print_error("No valid bag files found in the specified paths.")
            return 1

        time_ranges = []
        for bag_path in bag_files:
            try:
                start, end = self._get_start_end_timestamps(bag_path)
                time_ranges.append((start, end, bag_path))
            except Exception as e:
                print_error(f"Error processing {bag_path}: {e}")
                continue

        if not time_ranges:
            print_error("No valid bag files could be processed.")
            return 1

        overlap_start = max(start for start, _, _ in time_ranges)
        overlap_end = min(end for _, end, _ in time_ranges)

        if overlap_start > overlap_end:
            print("Warning: No temporal overlap found between the bag files.")
            overlap_start = datetime.now()
            overlap_end = datetime.now()

        print("\nBag File Summary:")
        print("=" * 80)
        for start, end, bag_path in time_ranges:
            self._print_bag_summary(start, end, bag_path)

        print("\nTemporal Overlap Summary:")
        print("=" * 80)
        self._print_timespan(overlap_start, overlap_end)

        if args.plot:
            try:
                self._plot_bag_time_ranges(time_ranges)
            except ImportError:
                print("Warning: Plotting functionality requires matplotlib. Install with: pip install matplotlib")

        return 0

    def _crop_bags(self, args) -> int:
        """Crop bag files to their overlap period and save to output directory."""
        # Get all bag files from the specified paths
        bag_files = self._get_all_bag_files(args.bags)
        
        if not bag_files:
            print_error("No valid bag files found in the specified paths.")
            return 1
            
        output_dir = args.output_dir
        overwrite = args.overwrite

        # Find overlap
        time_ranges = []
        for bag_path in bag_files:
            try:
                start, end = self._get_start_end_timestamps(bag_path)
                time_ranges.append((start, end, bag_path))
            except Exception as e:
                print_error(f"Error processing {bag_path}: {e}")
                continue

        if not time_ranges:
            print_error("No valid bag files could be processed.")
            return 1

        overlap_start = max(start for start, _, _ in time_ranges)
        overlap_end = min(end for _, end, _ in time_ranges)

        # Check if there's a valid overlap
        if overlap_start >= overlap_end:
            print_error("Cannot crop: No valid temporal overlap found.")
            return 1

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        success_count = 0
        for bag_path in bag_files:
            try:
                if self._crop_bag(bag_path, overlap_start, overlap_end, output_dir, overwrite):
                    success_count += 1
            except Exception as e:
                print_error(f"Error cropping {bag_path}: {e}")

        print(f"\nSuccessfully cropped {success_count}/{len(bag_files)} bags")
        return 0 if success_count > 0 else 1

    def _get_start_end_timestamps(self, bag_path: str) -> Tuple[datetime, datetime]:
        """Get the start and end timestamps of a bag file by parsing metadata.yaml."""
        
        # Determine if bag_path is a directory or file
        if os.path.isdir(bag_path):
            metadata_path = os.path.join(bag_path, "metadata.yaml")
            if not os.path.exists(metadata_path):
                raise ValueError(f"No metadata.yaml found in directory {bag_path}")
        else:
            # For single files, check if a metadata file exists in the parent directory
            parent_dir = os.path.dirname(bag_path)
            metadata_path = os.path.join(parent_dir, "metadata.yaml")
            if not os.path.exists(metadata_path):
                # Fall back to original method if no metadata file is found
                return self._get_timestamps_using_reader(bag_path)
    
        # Parse metadata.yaml
        try:
            with open(metadata_path, 'r') as f:
                metadata = yaml.safe_load(f)
            
            # Check if metadata is None or empty
            if metadata is None:
                print(f"Warning: metadata.yaml is empty or invalid. Falling back to reader method.")
                return self._get_timestamps_using_reader(bag_path)
            
            info = metadata.get('rosbag2_bagfile_information', {})
            
            # Get start time and duration in nanoseconds
            start_ns = info.get('starting_time', {}).get('nanoseconds_since_epoch', 0)
            duration_ns = info.get('duration', {}).get('nanoseconds', 0)
            end_ns = start_ns + duration_ns
            
            # Convert to datetime objects
            start_dt = datetime.fromtimestamp(start_ns / 1e9)
            end_dt = datetime.fromtimestamp(end_ns / 1e9)
            
            return start_dt, end_dt
            
        except (yaml.YAMLError, KeyError, TypeError, FileNotFoundError) as e:
            print(f"Warning: Error parsing metadata.yaml: {e}. Falling back to reader method.")
            return self._get_timestamps_using_reader(bag_path)

    def _get_timestamps_using_reader(self, bag_path: str) -> Tuple[datetime, datetime]:
        """Legacy method to get timestamps using SequentialReader (fallback)."""
        storage_id = None
        file_paths = []
        
        # Determine storage type and file paths
        if os.path.isdir(bag_path):
            db3_files = [os.path.join(bag_path, f) for f in os.listdir(bag_path) if f.endswith(".db3")]
            mcap_files = [os.path.join(bag_path, f) for f in os.listdir(bag_path) if f.endswith(".mcap")]
            
            if db3_files:
                storage_id = "sqlite3"
                file_paths = [db3_files[0]]  # For SQLite, we only need one file
            elif mcap_files:
                storage_id = "mcap"
                file_paths = mcap_files  # For MCAP, we need to process all files
            else:
                raise ValueError(f"No valid bag files found in directory {bag_path}")
        else:
            storage_id = "sqlite3" if bag_path.endswith(".db3") else "mcap"
            file_paths = [bag_path]
        
        # Process file(s) to find start and end times
        start_ns = float("inf")
        end_ns = float("-inf")
        
        for file_path in file_paths:
            storage_options = StorageOptions(uri=file_path, storage_id=storage_id)
            converter_options = ConverterOptions(
                input_serialization_format="cdr",
                output_serialization_format="cdr",
            )
            
            reader = SequentialReader()
            reader.open(storage_options, converter_options)
            metadata = reader.get_metadata()
            
            file_start = metadata.starting_time.nanoseconds
            file_end = file_start + metadata.duration.nanoseconds
            
            start_ns = min(start_ns, file_start)
            end_ns = max(end_ns, file_end)
        
        # Convert nanoseconds to datetime
        start_dt = datetime.fromtimestamp(start_ns / 1e9)
        end_dt = datetime.fromtimestamp(end_ns / 1e9)
        
        return start_dt, end_dt

    def _get_all_bag_files(self, paths: List[str]) -> List[str]:
        """Recursively process the input paths and return a list of all bag files/directories."""
        bag_files = []

        def is_bag_directory(dir_path):
            """Check if a directory is a valid bag directory."""
            if not os.path.isdir(dir_path):
                return False
            has_metadata = os.path.exists(os.path.join(dir_path, "metadata.yaml"))
            files = os.listdir(dir_path)
            has_db3 = any(f.endswith(".db3") for f in files)
            has_mcap = any(f.endswith(".mcap") for f in files)
            return has_metadata and (has_db3 or has_mcap)

        def find_bag_files_recursively(path, n):
            """Recursively find bag directories up to a specified depth."""
            if n < 0:
                return
            if is_bag_directory(path):
                bag_files.append(path)
            else:
                if os.path.isdir(path):
                    for item in os.listdir(path):
                        item_path = os.path.join(path, item)
                        if not os.path.isdir(item_path):
                            continue
                        find_bag_files_recursively(item_path, n - 1)

        for path in paths:
            if not os.path.exists(path):
                raise ValueError(f"Path {path} does not exist.")
            find_bag_files_recursively(path, n=1)

        print(f"Found {len(bag_files)} bag files/directories:")
        for bag in bag_files:
            print(f" - {bag}")

        return bag_files

    def _crop_bag(
        self,
        bag_path: str,
        overlap_start: datetime,
        overlap_end: datetime,
        output_dir: str,
        overwrite: bool = False,
    ) -> bool:
        """Crop a single bag file to the overlap period."""
        # Get the bag name for the output file
        bag_name = os.path.basename(bag_path.rstrip("/"))
        output_path = os.path.join(output_dir, f"{bag_name}_cropped")

        # Check if output directory exists and ask for overwrite
        if os.path.exists(output_path):
            if not overwrite:
                try:
                    response = input(
                        f"Bag directory {output_path} already exists. Overwrite? [Y/n] "
                    )
                    if response.lower() not in ["", "y", "yes"]:
                        print(f"Skipping {bag_name}")
                        return False
                except (EOFError, KeyboardInterrupt):
                    print(f"Skipping {bag_name}")
                    return False
            # Remove existing directory
            shutil.rmtree(output_path)

        # Prepare storage options
        if os.path.isdir(bag_path):
            # For directory-based bags, use the same storage format as input
            if any(f.endswith(".db3") for f in os.listdir(bag_path)):
                storage_id = "sqlite3"
            else:
                storage_id = "mcap"
        else:
            storage_id = "sqlite3" if bag_path.endswith(".db3") else "mcap"

        # Check if the bag file is completely contained in the overlap period
        bag_start, bag_end = self._get_start_end_timestamps(bag_path)
        if (
            overlap_start.timestamp() * 1e9 <= bag_start.timestamp() * 1e9 + 10
            and bag_end.timestamp() * 1e9 - 10 <= overlap_end.timestamp() * 1e9
        ):
            # copy the entire bag
            print(f"Bag {bag_path} is completely contained in the overlap period.")
            print(f"Copying {bag_path} to {output_path}")
            shutil.copytree(bag_path, output_path)
            return True

        # Create writer with max file size of 1GB
        writer = SequentialWriter()
        writer.open(
            StorageOptions(
                uri=output_path,
                storage_id=storage_id,
                max_bagfile_size=1024 * 1024 * 1024,  # 1GB in bytes
            ),
            ConverterOptions("", ""),
        )

        # Get total messages from metadata
        reader = SequentialReader()
        reader.open(
            StorageOptions(uri=bag_path, storage_id=storage_id),
            ConverterOptions("", ""),
        )

        metadata = reader.get_metadata()
        total_msgs = metadata.message_count
        print(f"Total messages: {total_msgs}")

        # Copy topic metadata
        for topic in reader.get_all_topics_and_types():
            writer.create_topic(topic)

        # Copy messages within overlap period
        copied_msg_count = 0
        skipped_msg_count = 0
        while reader.has_next():
            (topic, data, t) = reader.read_next()
            # t is already in nanoseconds
            if overlap_start.timestamp() * 1e9 <= t <= overlap_end.timestamp() * 1e9:
                writer.write(topic, data, t)
                copied_msg_count += 1
                if copied_msg_count % 1000 == 0:
                    print(
                        f"\rProgress: {copied_msg_count}/{total_msgs} messages ({copied_msg_count/total_msgs*100:.1f}%)",
                        end="",
                    )
            else:
                skipped_msg_count += 1

        print(f"\nCropped bag saved to: {output_path}")
        return True

    def _print_bag_summary(self, start: datetime, end: datetime, path: str) -> None:
        """Print the summary of a bag file."""
        print(f"\nBag: {path}")
        self._print_timespan(start, end)

    def _print_timespan(self, start: datetime, end: datetime) -> None:
        """Print timespan information."""
        print(f" Start:    {start}")
        print(f" End:      {end}")
        print(f" Duration: {end - start}")

    def _plot_bag_time_ranges(self, time_ranges):
        """Plot the time ranges of the bag files."""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            import matplotlib.ticker as mticker
            import math
            from datetime import timedelta
        except ImportError:
            raise ImportError("Plotting requires matplotlib. Install with: pip install matplotlib")

        fig, ax = plt.subplots(figsize=(12, 6))

        # Calculate overall time span
        all_times = [start for start, _, _ in time_ranges] + [end for _, end, _ in time_ranges]
        min_time = min(all_times)
        max_time = max(all_times)
        total_span = max_time - min_time
        total_seconds = max(total_span.total_seconds(), 0.0)

        # Plot each bag's time range using Matplotlib date numbers (in days)
        for i, (start, end, path) in enumerate(time_ranges):
            bag_name = os.path.basename(path.rstrip("/"))
            start_num = mdates.date2num(start)
            end_num = mdates.date2num(end)
            width_days = end_num - start_num
            if width_days == 0:
                # ensure visibility for zero-duration ranges (use 1 second width)
                width_days = 1.0 / 86400.0
            ax.barh(
                i,
                width_days,
                left=start_num,
                height=0.6,
                alpha=0.7,
                label=bag_name,
            )

        # Format the plot
        ax.set_yticks(range(len(time_ranges)))
        ax.set_yticklabels([os.path.basename(path.rstrip("/")) for _, _, path in time_ranges])
        ax.set_xlabel('Time')
        ax.set_title('Bag File Time Ranges')
        ax.invert_yaxis()  # First input bag at top, last at bottom

        # Set x-limits with a small padding first (so locator uses correct view limits)
        min_num = mdates.date2num(min_time)
        max_num = mdates.date2num(max_time)
        span_days = max(max_num - min_num, 0.0)
        if span_days <= 0:
            span_days = 1.0 / 1440.0  # 1 minute
        pad_days = span_days * 0.02
        # Temporarily disable locators to prevent AutoDateLocator from generating excessive ticks
        ax.xaxis.set_major_locator(mticker.NullLocator())
        ax.xaxis.set_minor_locator(mticker.NullLocator())
        ax.set_xlim(min_num - pad_days, max_num + pad_days)
        ax.xaxis_date()

        # Use a manual locator/formatter selection targeting ~8 ticks to avoid too few (e.g., only years)
        min_ticks, target, max_ticks = 4, 8, 10
        seconds = total_seconds if total_seconds > 0 else 1.0
        # unit name, seconds per unit, locator factory, formatter string
        units = [
            ("year",   365 * 86400.0, lambda n: mdates.YearLocator(base=int(n)),   '%Y'),
            ("month",   30 * 86400.0, lambda n: mdates.MonthLocator(interval=int(n)), '%Y-%m'),
            ("day",           86400.0, lambda n: mdates.DayLocator(interval=int(n)),   '%m-%d'),
            ("hour",            3600.0, lambda n: mdates.HourLocator(interval=int(n)),  '%m-%d %H:%M'),
            ("minute",            60.0, lambda n: mdates.MinuteLocator(interval=int(n)), '%H:%M'),
            ("second",             1.0, lambda n: mdates.SecondLocator(interval=int(n)), '%H:%M:%S'),
        ]

        best = None           # (score, locator, fmt, ticks)
        best_over = None      # minimal ticks above max
        best_under = None     # maximal ticks below min

        for _, unit_sec, factory, fmt_str in units:
            count = seconds / unit_sec
            interval = max(1, int(math.ceil(count / target)))
            ticks = int(max(2, math.floor(count / interval) + 1))

            if min_ticks <= ticks <= max_ticks:
                score = abs(ticks - target)
                cand = (score, factory(interval), fmt_str, ticks)
                if best is None or score < best[0]:
                    best = cand
            elif ticks > max_ticks:
                cand = (ticks, factory(interval), fmt_str, ticks)
                if best_over is None or ticks < best_over[0]:
                    best_over = cand
            else:  # ticks < min_ticks
                cand = (ticks, factory(interval), fmt_str, ticks)
                if best_under is None or ticks > best_under[0]:
                    best_under = cand

        if best is not None:
            locator = best[1]
            fmt = best[2]
        elif best_over is not None:
            locator = best_over[1]
            fmt = best_over[2]
        elif best_under is not None:
            locator = best_under[1]
            fmt = best_under[2]
        else:
            locator = mdates.MonthLocator(interval=2)
            fmt = '%Y-%m'

        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
        # Disable minor ticks entirely to avoid massive minor tick generation
        ax.xaxis.set_minor_locator(mticker.NullLocator())

        # Rotate labels
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_horizontalalignment('right')

        # Highlight overlap region if it exists
        overlap_start = max(start for start, _, _ in time_ranges)
        overlap_end = min(end for _, end, _ in time_ranges)
        if overlap_start < overlap_end:
            ax.axvspan(
                mdates.date2num(overlap_start),
                mdates.date2num(overlap_end),
                alpha=0.3,
                color='red',
                label='Overlap Region',
            )

        plt.tight_layout()
        plt.show()
