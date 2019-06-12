from bin.network.Segment import Segment


class Node:

    id = 0
    segments = None
    start_node = True
    end_node = True
    coords = []

    def __init__(self, id):
        self.id = id
        self.segments = []

    def get_upstream_segments(self):
        up_segments = []
        for segment in self.segments:
            if isinstance(segment, Segment) and segment.to_node == self:
                up_segments.append(segment)
        return up_segments

    def get_downstream_segments(self):
        down_segments = []
        for segment in self.segments:
            if isinstance(segment, Segment) and segment.from_node == self:
                down_segments.append(segment)
        return down_segments
