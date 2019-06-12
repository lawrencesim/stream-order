import csv
import common.feature_utils as feature_utils
from bin.PassPrint import PassPrint
from shared import _get_segments, _parse_nodes


printer = None


def calculate_flow(stream_dataset, stream_id_column, node_dataset_or_table, drainage_node_ids, from_node_column,
                   to_node_column, braided_column, output_flow_table_path):
    global printer
    if not printer:
        printer = PassPrint()

    # get segments
    printer.msg("Creating segments..")
    segments = _get_segments(stream_dataset, stream_id_column)
    # get node network
    printer.msg("Creating node network..")
    nodes, drainage_nodes = _parse_nodes(node_dataset_or_table, drainage_node_ids=drainage_node_ids, require_drainage=True)

    # maps by id
    segment_map = {}
    for segment in segments:
        segment_map[segment.id] = segment
    node_map = {}
    for node in nodes:
        node_map[node.id] = node
        # all nodes assumed connected to start
        node.end_node = False
        node.start_node = False

    printer.msg("Calculating flow..")

    # map first segments to drainage nodes
    active_segments = []
    for node in drainage_nodes:
        # drainage node is end node
        node.end_node = True
        # for each segment linked
        for s in range(len(node.segments)):
            sid = node.segments[s]
            # replace with actual segment object
            segment = segment_map[sid]
            if not segment:
                raise Exception("Could not find segment ({0}) for node ({1})".format(node.id, sid))
            node.segments[s] = segment
            # link objects (since drainage, always to-relationship)
            segment.to_node = node
            # add to active
            active_segments.append(segment)
        # remove node from map
        del node_map[node.id]

    braided_count = 0

    # now loop until no more active segments found
    while len(active_segments):
        active_nodes = []

        for segment in active_segments:
            # possible case with duplicates for braided streams
            if segment.from_node:
                segment.braided = True
                braided_count += 1
            else:
                # find upstream node (can only be one)
                from_node = None
                from_node_seg_i = -1
                mapped_nodes = []
                for node in node_map.values():
                    for s in range(len(node.segments)):
                        # upstream node is node where linked segment is not yet properly attached to instance (just id)
                        if isinstance(node.segments[s], int):
                            if node.segments[s] == segment.id:
                                from_node = node
                                from_node_seg_i = s
                                break
                        # possible case of splitting stream where segment is already attached as flow-from
                        elif node.segments[s] == segment:
                            mapped_nodes.append(node)
                            if node != segment.to_node:
                                from_node = node
                                break
                    if from_node:
                        break
                # in complex braided streams this can happen
                if not from_node:
                    printer.error("Error finding upstream node for segment ({0})".format(segment.id))
                    printer.error("  Often due to complex braided networks. Streams may be to be cleaned and reprocesses.")
                    raise Exception("Error calculating network flow.")
                # make upstream relationship
                segment.from_node = from_node
                if from_node_seg_i >= 0:
                    from_node.segments[from_node_seg_i] = segment
                # add to active nodes
                active_nodes.append(from_node)
                # trim down connected elements from maps (for nodes, first check every segment relationship has been
                # matched as braided streams means there may be downstream split)
                del segment_map[segment.id]
                delete_node = True
                for s in from_node.segments:
                    if isinstance(s, int):
                        delete_node = False
                        break
                if delete_node:
                    del node_map[from_node.id]

        # error check (may occur with complex braided streams until I fix it)
        for segment in active_segments:
            if not segment.from_node:
                raise Exception("Could not match upstream node for segment ({0})".format(segment.id))

        # check for braided streams
        for node in active_nodes:
            feeds_segments = []
            for segment in active_segments:
                if segment.from_node == node:
                    feeds_segments.append(segment)
            if len(feeds_segments) > 1:
                for segment in feeds_segments:
                    segment.braided = True

        # clear active segments
        active_segments = []
        # find upstream segments for next set of nodes
        for node in active_nodes:
            connected_segments = []
            for segment in segment_map.values():
                for s in range(len(node.segments)):
                    # check if segment matches
                    if isinstance(node.segments[s], int) and node.segments[s] == segment.id:
                        node.segments[s] = segment
                        # add downstream (to segment) relationship
                        if not segment.to_node:
                            segment.to_node = node
                            connected_segments.append(segment)
                        # unless some other branch already added it as downstream, if upstream is free, mark as braided
                        elif not segment.from_node:
                            segment.from_node = node
                            segment.braided = True
                        # otherwise this braided network is too confusing to solve
                        else:
                            printer.error("Error finding upstream node for segment ({0})".format(segment.id))
                            printer.error("  Often due to complex braided networks. Streams may be to be cleaned and reprocesses.")
                            raise Exception("Error calculating network flow.")
                        break
            # if no upstream segment, mark node as starting node
            if not len(connected_segments):
                node.start_node = True
            else:
                active_segments += connected_segments

    # for warnings later
    unconnected_segments = segment_map.keys()

    if output_flow_table_path:
        printer.msg("Saving output table..")
        with open(output_flow_table_path, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([stream_id_column, from_node_column, to_node_column, braided_column])
            for segment in segments:
                from_node_id = segment.from_node.id if segment.from_node else ""
                to_node_id = segment.to_node.id if segment.to_node else ""
                writer.writerow([segment.id, from_node_id, to_node_id, 1 if segment.braided else 0])

    printer.msg("Adding new attributes to shapefile")

    stream_ds = feature_utils.getFeatureDataset(stream_dataset, write=True)
    stream_layer = stream_ds.GetLayer()
    stream_defn = stream_layer.GetLayerDefn()

    index_stream_id = stream_defn.GetFieldIndex(stream_id_column)
    if index_stream_id < 0:
        return Exception("Could not find column ({0})".format(stream_id_column))
    stream_id = stream_defn.GetFieldDefn(index_stream_id)

    index_from_node = stream_defn.GetFieldIndex(from_node_column)
    if index_from_node < 0:
        printer.msg("  creating column {0}".format(from_node_column))
        from_node = feature_utils.createFieldDefinition(from_node_column, int)
        stream_layer.CreateField(from_node)

    index_to_node = stream_defn.GetFieldIndex(to_node_column)
    if index_to_node < 0:
        printer.msg("  creating column {0}".format(to_node_column))
        to_node = feature_utils.createFieldDefinition(to_node_column, int)
        stream_layer.CreateField(to_node)

    index_braided = stream_defn.GetFieldIndex(braided_column)
    if index_braided < 0:
        printer.msg("  creating column {0}".format(braided_column))
        braided = feature_utils.createFieldDefinition(braided_column, int)
        stream_layer.CreateField(braided)

    segment_map = {}
    for segment in segments:
        segment_map[segment.id] = segment

    printer.msg("  writing..")
    stream_feat = stream_layer.GetNextFeature()
    while stream_feat:
        sid = feature_utils.getFieldValue(stream_feat, stream_id)
        segment = segment_map[sid]
        if segment.from_node:
            stream_feat.SetField(from_node_column, segment.from_node.id)
        if segment.to_node:
            stream_feat.SetField(to_node_column, segment.to_node.id)
        stream_feat.SetField(braided_column, 1 if segment.braided else 0)
        stream_layer.SetFeature(stream_feat)
        stream_feat = stream_layer.GetNextFeature()

    stream_defn = None
    stream_layer = None
    stream_ds = None

    # print warnings last
    # check for unconnected elements and throw up warnings
    if len(unconnected_segments):
        printer.warn(
            "WARNING: Unconnected segments in stream network. Assign drainage point to unconnected branches or remove them.",
            indent=0
        )
        printer.warn(
            "  {0}: {1}".format(stream_id_column, ", ".join([str(sid) for sid in unconnected_segments])),
            indent=0
        )
    # warning if braided/looping streams possible in network
    if braided_count > 0:
        printer.warn(
            "WARNING: Possible braided stream errors, check outputs and correct as needed before proceeding.",
            indent=0
        )
