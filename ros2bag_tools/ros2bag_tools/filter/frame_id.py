from rclpy.serialization import deserialize_message
from rclpy.serialization import serialize_message

from ros2bag_tools.filter import FilterExtension

from rosidl_runtime_py import set_message_fields
from rosidl_runtime_py.utilities import get_message

import yaml


class FrameIdFilter(FilterExtension):

    def __init__(self):
        self._args = None
        self._msg_module = None
        self._values_dictionary = {}

    def add_arguments(self, parser):
        parser.add_argument('-t','--topic', action='append', required=True, help='topic to replace data for (can be repeated)')
        parser.add_argument('--frame_id', action='append', help='frame_id replacement as old:new (can be repeated, matches --topic order)')
        parser.add_argument('--child_frame_id', action='append', help='child_frame_id replacement as old:new (can be repeated, matches --topic order)')

    def set_args(self, _metadata, args):
        self._args = args
        if args.frame_id is None and args.child_frame_id is None:
            raise RuntimeError('At least one of --frame_id or --child_frame_id must be specified')
        self._topic_map = {}
        num_topics = len(args.topic)
        frame_ids = args.frame_id or [None] * num_topics
        child_frame_ids = args.child_frame_id or [None] * num_topics
        for i, topic in enumerate(args.topic):
            mapping = {}
            if frame_ids[i]:
                if ':' not in frame_ids[i]:
                    raise RuntimeError('frame_id must be specified as old:new')
                old, new = frame_ids[i].split(':', 1)
                mapping['frame_id'] = (old, new)
            if child_frame_ids[i]:
                if ':' not in child_frame_ids[i]:
                    raise RuntimeError('child_frame_id must be specified as old:new')
                old, new = child_frame_ids[i].split(':', 1)
                mapping['child_frame_id'] = (old, new)
            self._topic_map[topic] = mapping

    def filter_topic(self, topic_metadata):
        topic = topic_metadata.name
        if topic in self._topic_map:
            try:
                self._msg_module = get_message(topic_metadata.type)
            except (AttributeError, ModuleNotFoundError, ValueError):
                raise RuntimeError('The passed message type is invalid')
        return topic_metadata

    def filter_msg(self, msg):
        (topic, data, t) = msg
        if topic in getattr(self, '_topic_map', {}):
            if not self._msg_module:
                raise RuntimeError(f"Could not load message type of topic '{topic}'")
            msg = deserialize_message(data, self._msg_module)
            mapping = self._topic_map[topic]
            # Patch header.frame_id if specified
            if 'frame_id' in mapping and hasattr(msg, 'header') and hasattr(msg.header, 'frame_id'):
                old, new = mapping['frame_id']
                if old == '*' or msg.header.frame_id == old:
                    msg.header.frame_id = new
            # Patch child_frame_id if specified
            if 'child_frame_id' in mapping and hasattr(msg, 'child_frame_id'):
                old, new = mapping['child_frame_id']
                if old == '*' or msg.child_frame_id == old:
                    msg.child_frame_id = new
            return (topic, serialize_message(msg), t)
        return msg
