from ros2bag_tools.filter.frame_id import FrameIdFilter
from ros2bag_tools.verb import FilterVerb


class FrameIdVerb(FilterVerb):
    """Replace header.frame_id of messages in a bag with a new value."""

    def __init__(self):
        FilterVerb.__init__(self, FrameIdFilter())
