from bin.PassPrint import PassPrint
from common import feature_utils, raster_utils
from shared import _get_segments, _parse_nodes
from gdal import osr


printer = None


def complete_braided_streams(stream_dataset, stream_id_column, node_dataset_or_table, from_node_column, to_node_column,
                             braided_column, elevation_dataset):
    global printer
    if not printer:
        printer = PassPrint()

    # get node network
    printer.msg("Reading node network..")
    nodes, drainage_nodes = _parse_nodes(node_dataset_or_table, get_coords=(True if elevation_dataset else False))
    # original nodes map
    og_nodes_map = {}
    for node in nodes:
        og_nodes_map[node.id] = node

    # read streams layer and get segments
    printer.msg("Reading streams dataset..")
    stream_dataset = feature_utils.getFeatureDataset(stream_dataset, write=1)
    stream_layer = stream_dataset.GetLayer()
    stream_defn = stream_layer.GetLayerDefn()
    stream_srs = stream_layer.GetSpatialRef()

    index_stream_id = stream_defn.GetFieldIndex(stream_id_column)
    if index_stream_id < 0:
        return Exception("Could not find column ({0})".format(stream_id_column))
    stream_id_col = stream_defn.GetFieldDefn(index_stream_id)
    index_from_node = stream_defn.GetFieldIndex(from_node_column)
    if index_from_node < 0:
        return Exception("Could not find column ({0})".format(from_node_column))
    index_to_node = stream_defn.GetFieldIndex(to_node_column)
    if index_to_node < 0:
        return Exception("Could not find column ({0})".format(to_node_column))

    printer.msg("Creating segments..")
    segments = _get_segments(stream_dataset, stream_id_column, nodes, from_node_column, to_node_column, braided_column)

    # get braided segments
    braided_segments = []
    for segment in segments:
        if segment.braided:
            braided_segments.append(segment)

    if not len(braided_segments):
        printer.msg("No braided segments labeled to complete.")
        return

    # optional elevation
    dem = None
    dem_transform = None
    if elevation_dataset:
        printer.msg("Reading elevation dataset..")
        dem = raster_utils.getRasterAsGdal(elevation_dataset)
        dem_srs = raster_utils.getRasterSpatialReference(dem)
        dem_transform = osr.CoordinateTransformation(stream_srs, dem_srs)

    try:
        printer.msg("Completing braided networks..")
        braided_segment_ids = []
        for initial_segment in braided_segments:

            # if DEM exists, check whether to swap to/from node
            if dem:
                nodes = [initial_segment.from_node, initial_segment.to_node]
                node_els = [0, 0]
                for i in range(2):
                    node_coords = dem_transform.TransformPoint(nodes[i].coords[0], nodes[i].coords[1])
                    node_pixels = raster_utils.calcPixelCoordinate(node_coords, dataset=dem)
                    node_els[i] = raster_utils.readRaster(dem, 1, node_pixels[0], node_pixels[1])
                # swap from/to nodes
                if node_els[0] < node_els[1]:
                    initial_segment.from_node = nodes[1]
                    initial_segment.to_node = nodes[0]
                    # make changes to shapefile with callback
                    def update_segment_nodes(feat):
                        if feature_utils.getFieldValue(feat, stream_id_col) == initial_segment.id:
                            feat.SetField(index_from_node, initial_segment.from_node.id)
                            feat.SetField(index_to_node, initial_segment.to_node.id)
                            stream_layer.SetFeature(feat)
                            return True
                    feature_utils.forEachFeature(stream_layer, update_segment_nodes)

            source_node = initial_segment.from_node

            # as we run down network, for each split, value is equally split / for each merge, values added up
            start_value = 1000000.0  # start with high number to avoid fraction issues
            tolerance = 0.0001
            fraction_by_node = {source_node.id: start_value}
            fraction_by_segment = {}

            # we also keep a list of touched node ids, so we ignore other feeder segments that appear when traversing back
            # up the sub-network later
            touched_nids = [source_node.id]

            # search down from starting node, tracking values
            active_nodes = [source_node]
            end_node = None
            while len(active_nodes):
                next_active_nodes = []

                for node in active_nodes:
                    # get feeding node fraction value
                    node_fraction = fraction_by_node[node.id]
                    # for downstream segments
                    downstream = node.get_downstream_segments()
                    if len(downstream):
                        fraction_value = node_fraction / len(downstream)
                        for segment in downstream:
                            # add fraction value from feeding node
                            if segment.id not in fraction_by_segment:
                                fraction_by_segment[segment.id] = fraction_value
                            else:
                                fraction_by_segment[segment.id] += fraction_value
                            # get next set of downstream nodes
                            if segment.to_node not in next_active_nodes:
                                next_active_nodes.append(segment.to_node)

                for node in next_active_nodes:
                    # add new node id
                    touched_nids.append(node.id)
                    # update this active node layer's fraction values
                    node_fraction = 0
                    for segment in node.get_upstream_segments():
                        if segment.id in fraction_by_segment:
                            node_fraction += fraction_by_segment[segment.id]
                    # if at original value (with tolerance due to bitmath), we've found common end point
                    if abs(node_fraction - start_value) < tolerance:
                        end_node = node
                        break
                    # update value
                    fraction_by_node[node.id] = node_fraction
                # propagate break condition
                if end_node:
                    break

                active_nodes = next_active_nodes

            if not end_node:
                printer.error("Could not find shared end node for all assumed braided flows out of start node={0}. Complex networks with braided and unbraided streams from common start node should be classified manually)".format(source_node.id))
                raise Exception("Could not parse braided stream network.")

            # traverse upstream from end node, ignoring those nodes that weren't processed going downstream, keeping list of
            # unique node ids identified as part of this braided network
            active_nodes = [end_node]
            while len(active_nodes):
                next_active_nodes = []

                for node in active_nodes:
                    for segment in node.get_upstream_segments():
                        if segment.from_node.id not in touched_nids:
                            continue
                        if segment.id not in braided_segment_ids:
                            braided_segment_ids.append(segment.id)
                        if segment.from_node not in next_active_nodes:
                            next_active_nodes.append(segment.from_node)

                active_nodes = next_active_nodes

        dem = None
        if not len(braided_segment_ids):
            return

        printer.msg("Updating streams..")

        index_braided = stream_defn.GetFieldIndex(braided_column)
        if index_braided < 0:
            return Exception("Could not find column ({0})".format(braided_column))

        stream_feat = stream_layer.GetNextFeature()
        while stream_feat:
            sid = feature_utils.getFieldValue(stream_feat, stream_id_col)
            if sid in braided_segment_ids:
                stream_feat.SetField(braided_column, 1)
            stream_layer.SetFeature(stream_feat)
            stream_feat = stream_layer.GetNextFeature()

    finally:
        dem = None
        node_layer = None
        node_dataset = None
        stream_layer = None
        stream_dataset = None
