class Segment:

    id = 0
    from_node = None
    to_node = None
    order = -1
    braided = False

    def __init__(self, id, from_node=None, to_node=None):
        self.id = id
        if from_node:
            self.connect_from_node(from_node)
        if to_node:
            self.connect_to_node(to_node)

    def connect_from_node(self, from_node):
        self.from_node = from_node
        self.from_node.segments.append(self)
        self.from_node.end_node = False

    def connect_to_node(self, to_node):
        self.to_node = to_node
        self.to_node.segments.append(self)
        self.to_node.start_node = False
