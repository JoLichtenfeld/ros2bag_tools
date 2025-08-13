import os
import subprocess
import tempfile
import yaml
import glob
from pathlib import Path

from ros2bag.verb import VerbExtension
from ros2bag.api import print_error


class DecompressVerb(VerbExtension):
    """Decompress ROS2 bags by re-writing them without compression."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'input_bags',
            nargs='+',
            help='Path(s) to input ROS2 bag directory/directories (supports glob patterns)'
        )
        parser.add_argument(
            '-o', '--output',
            default=None,
            help='Output bag name (only for single input bag, default: <input>_decompressed)'
        )
        parser.add_argument(
            '--max-size',
            type=int,
            default=1000000000,
            help='Maximum bagfile size in bytes (default: 1GB)'
        )
        parser.add_argument(
            '--validate',
            action='store_true',
            default=True,
            help='Validate message count after decompression (default: True)'
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
            help='Print the YAML configuration used for decompression'
        )

    def main(self, *, args):
        # Expand glob patterns and validate inputs
        input_bags = []
        for pattern in args.input_bags:
            pattern = pattern.rstrip('/')
            matches = glob.glob(pattern)
            if matches:
                for match in matches:
                    if self._is_valid_bag(match):
                        input_bags.append(match)
                    else:
                        print(f"Warning: Skipping '{match}' - not a valid ROS2 bag")
            else:
                if self._is_valid_bag(pattern):
                    input_bags.append(pattern)
                elif os.path.exists(pattern):
                    print_error(f"'{pattern}' exists but is not a valid ROS2 bag")
                else:
                    print_error(f"Input bag '{pattern}' not found")

        if not input_bags:
            print_error('No valid bag files found')
            return 1

        if args.output and len(input_bags) > 1:
            print_error('Cannot specify output name (-o) when processing multiple bags')
            return 1

        print(f"Found {len(input_bags)} valid bag(s) to decompress")
        success = 0
        for input_bag in input_bags:
            output_bag = args.output if args.output else f"{input_bag}_decompressed"
            if self._decompress_single_bag(input_bag, output_bag, args):
                success += 1
            else:
                print_error(f"Failed to decompress: {input_bag}")

        if len(input_bags) > 1:
            print(f"\nDecompression summary: {success}/{len(input_bags)} succeeded")
        return 0 if success == len(input_bags) else 1

    def _is_valid_bag(self, bag_path):
        if not os.path.isdir(bag_path):
            return False
        if not os.path.exists(os.path.join(bag_path, 'metadata.yaml')):
            return False
        data_files = glob.glob(os.path.join(bag_path, '*.mcap')) + glob.glob(os.path.join(bag_path, '*.db3'))
        return len(data_files) > 0

    def _decompress_single_bag(self, input_bag, output_bag, args):
        original_info = None
        original_messages = None
        try:
            if args.validate:
                res = subprocess.run(['ros2', 'bag', 'info', input_bag], capture_output=True, text=True, check=True)
                original_info = res.stdout
                for line in original_info.splitlines():
                    if 'Messages:' in line:
                        original_messages = int(line.split(':', 1)[1].strip())
                        break
        except Exception:
            print('Warning: could not read original message count')

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            cfg = {
                'output_bags': [{
                    'uri': output_bag,
                    'max_bagfile_size': args.max_size,
                    'all_topics': True
                }]
            }
            yaml.dump(cfg, tmp, default_flow_style=False)
            cfg_path = tmp.name

        if args.verbose:
            print(f"\nYAML Configuration for {input_bag}:")
            print('=' * 50)
            print(yaml.dump(cfg, default_flow_style=False))
            print('=' * 50)

        print(f"Decompressing: {input_bag} -> {output_bag}")
        try:
            run = subprocess.run(['ros2', 'bag', 'convert', '-i', input_bag, '-o', cfg_path], capture_output=True, text=True)
            if run.returncode != 0:
                print_error('ros2 bag convert failed')
                print_error(run.stderr)
                return False

            if not os.path.isdir(output_bag):
                print_error('Output bag not created')
                return False

            # Validate message count
            if args.validate and original_messages is not None:
                out_info = subprocess.run(['ros2', 'bag', 'info', output_bag], capture_output=True, text=True)
                if out_info.returncode != 0:
                    print_error('Failed to inspect output bag')
                    return False
                compressed_messages = None
                for line in out_info.stdout.splitlines():
                    if 'Messages:' in line:
                        compressed_messages = int(line.split(':', 1)[1].strip())
                        break
                if compressed_messages is not None and compressed_messages != original_messages:
                    print_error(f"Message count mismatch: {original_messages} vs {compressed_messages}")
                    return False
                size_before = self._extract_bag_size(original_info)
                size_after = self._extract_bag_size(out_info.stdout)
                if size_before and size_after:
                    if size_after > 0:
                        expansion = (size_after / size_before) * 100
                        print(f"✅ Success: {original_messages} messages | size {self._format_size(size_before)} -> {self._format_size(size_after)} ({expansion:.1f}% of original)")
                    else:
                        print(f"✅ Success: {original_messages} messages | size {self._format_size(size_before)} -> {self._format_size(size_after)}")
                else:
                    print(f"✅ Success: {original_messages} messages preserved")
            else:
                print('✅ Decompression completed')
            return True
        finally:
            try:
                os.unlink(cfg_path)
            except OSError:
                pass

    def _extract_bag_size(self, bag_info):
        try:
            for line in bag_info.splitlines():
                if 'Bag size:' in line:
                    parts = line.split()
                    if len(parts) >= 4:
                        val = float(parts[2])
                        unit = parts[3]
                        if unit == 'GiB':
                            return int(val * 1024 ** 3)
                        if unit == 'MiB':
                            return int(val * 1024 ** 2)
                        if unit == 'KiB':
                            return int(val * 1024)
                        return int(val)
        except Exception:
            return None
        return None

    def _format_size(self, size_bytes):
        if size_bytes >= 1024 ** 3:
            return f"{size_bytes / (1024 ** 3):.1f} GiB"
        if size_bytes >= 1024 ** 2:
            return f"{size_bytes / (1024 ** 2):.1f} MiB"
        if size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KiB"
        return f"{size_bytes} bytes"
