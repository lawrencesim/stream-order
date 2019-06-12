import csv
import common.feature_utils as feature_utils
from shared import _get_segments, _parse_nodes
from bin.PassPrint import PassPrint
from bin.network.Node import Node
from bin.network.Segment import Segment


printer = None


def calculate_stream_order(stream_dataset, stream_id_column, from_node_column, to_node_column, braided_column,
                           stream_order_column, output_table_path):
    global printer
    if not printer:
        printer = PassPrint()

    printer.msg("Creating segments..")
    all_segments = _get_segments(stream_dataset, stream_id_column, from_node_column=from_node_column,
                                 to_node_column=to_node_column, braided_column=braided_column)

    printer.msg("Creating node network..")
    all_nodes, first_order_nodes = _get_node_network(all_segments)

    printer.msg("Assigning stream order..")
    _assign_stream_order(first_order_nodes)

    if output_table_path:
        printer.msg("Saving stream order table..")
        with open(output_table_path, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["STREAM_ID", "STRAHLER_STREAM_ORDER"])
            for segment in all_segments:
                writer.writerow([segment.id, segment.order])

    printer.msg("Updating streams..")
    segment_map = {}
    for segment in all_segments:
        segment_map[segment.id] = segment

    streams_ds = feature_utils.getFeatureDataset(stream_dataset, write=True)
    streams_layer = streams_ds.GetLayer()
    streams_defn = streams_layer.GetLayerDefn()

    f_index = streams_defn.GetFieldIndex(stream_id_column)
    if f_index < 0:
        raise Exception("Could not find column ({0})".format(stream_id_column))
    field_stream_id = streams_defn.GetFieldDefn(f_index)
    f_index = streams_defn.GetFieldIndex(stream_order_column)
    if f_index < 0:
        printer.msg("  creating column {0}".format(stream_order_column))
        field_stream_order = feature_utils.createFieldDefinition(stream_order_column, int)
        streams_layer.CreateField(field_stream_order)

    stream_feat = streams_layer.GetNextFeature()
    while stream_feat:
        stream_id = feature_utils.getFieldValue(stream_feat, field_stream_id)
        if stream_id in segment_map:
            stream_feat.SetField(stream_order_column, segment_map[stream_id].order)
        streams_layer.SetFeature(stream_feat)
        stream_feat = streams_layer.GetNextFeature()

    streams_defn = None
    streams_layer = None
    streams_ds = None


def _get_node_network(all_segments):
    # get unique node ids
    nodes = []
    node_ids = []
    for segment in all_segments:
        # match from node
        if segment.from_node not in node_ids:
            from_node = Node(segment.from_node)
            node_ids.append(segment.from_node)
            nodes.append(from_node)
        else:
            from_node = None
            for node in nodes:
                if node.id == segment.from_node:
                    from_node = node
                    break
        segment.connect_from_node(from_node)
        # match to node
        if segment.to_node not in node_ids:
            to_node = Node(segment.to_node)
            node_ids.append(segment.to_node)
            nodes.append(to_node)
        else:
            to_node = None
            for node in nodes:
                if node.id == segment.to_node:
                    to_node = node
                    break
        segment.connect_to_node(to_node)
    # find drainage points
    first_order_nodes = []
    for node in nodes:
        if not len(node.get_upstream_segments()):
            first_order_nodes.append(node)
    return nodes, first_order_nodes


def _assign_stream_order(first_order_nodes):
    # do first length
    active_nodes = []
    for node in first_order_nodes:
        for s in node.segments:
            s.order = 1
            if s.to_node not in active_nodes:
                active_nodes.append(s.to_node)

    # keep looping until no more active nodes
    while len(active_nodes):
        next_nodes = []
        # for each node
        for node in active_nodes:
            # get downstream segments, if none, skip
            downstream_segments = node.get_downstream_segments()
            if not len(downstream_segments):
                continue

            # get all upstream segment stream orders
            can_assign_value = True
            upstream_segment_values = []
            for s in node.get_upstream_segments():
                # if not all upstream segments have order, can't assign yet
                if s.order <= 0:
                    can_assign_value = False
                    break
                else:
                    upstream_segment_values.append([s.order, s.braided])

            # if can't assign value, just keep it in active list and continue
            if not can_assign_value:
                next_nodes.append(node)
                continue

            # get new stream order
            count_upstream = len(upstream_segment_values)
            if count_upstream == 0:
                # no upstream, first order
                stream_order = 1
            elif count_upstream == 1:
                # only one upstream, inherit order
                stream_order = upstream_segment_values[0][0]
            else:
                sorted_braided = sorted([x[0] for x in upstream_segment_values if x[1]])
                sorted_regular = sorted([x[0] for x in upstream_segment_values if not x[1]])
                # highest braided values
                value_if_braided = sorted_braided[-1] if len(sorted_braided) else 0
                # get expected value if ignoring braided values
                value_if_regular = 0
                if len(sorted_regular):
                    if len(sorted_regular) > 1 and sorted_regular[-1] == sorted_regular[-2]:
                        value_if_regular = sorted_regular[-1] + 1
                    else:
                        value_if_regular = sorted_regular[-1]
                if value_if_regular == value_if_braided:
                    # if value is equal for each time, increment order
                    stream_order = value_if_regular + 1
                else:
                    # value is highest of either
                    stream_order = value_if_regular if value_if_regular > value_if_braided else value_if_braided

            # assign stream order to all downstream segments
            for s in downstream_segments:
                s.order = stream_order
                # append next set of nodes to process
                if s.to_node not in next_nodes:
                    active_nodes.append(s.to_node)

        active_nodes = next_nodes
