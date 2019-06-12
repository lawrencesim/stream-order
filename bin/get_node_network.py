import csv
import ogr
import common.feature_utils as feature_utils
from bin.PassPrint import PassPrint
from bin.network.Node import Node
from bin.shared import _check_srs_units


printer = None


def get_node_network(stream_dataset, stream_id_column, output_node_shp, output_table_path, output_table_rev_path, tolerance):
    global printer
    if not printer:
        printer = PassPrint()

    printer.msg("Opening dataset..")
    streams_ds = feature_utils.getFeatureDataset(stream_dataset)
    streams_layer = streams_ds.GetLayer()
    streams_srs = streams_layer.GetSpatialRef()
    streams_defn = streams_layer.GetLayerDefn()

    _check_srs_units(srs=streams_srs)

    # get field indices
    id_field_index = streams_defn.GetFieldIndex(stream_id_column)
    if id_field_index < 0:
        raise Exception("Could not find stream ID column ({0})".format(stream_id_column))
    id_field_defn = streams_defn.GetFieldDefn(id_field_index)

    all_nodes = []
    all_stream_ids = []
    # all_feats = []
    current_id = 1

    # process all nodes at end points of all lines
    printer.msg("Creating nodes..")
    stream_feat = streams_layer.GetNextFeature()
    while stream_feat:
        stream_id = feature_utils.getFieldValue(stream_feat, id_field_defn)
        all_stream_ids.append(stream_id)

        geom = stream_feat.GetGeometryRef()
        points = geom.GetPoints()

        first_node = Node(current_id)
        current_id += 1
        first_node.coords = points[0]
        first_node.segments.append(stream_id)
        all_nodes.append(first_node)

        last_node = Node(current_id)
        current_id += 1
        last_node.coords = points[-1]
        last_node.segments.append(stream_id)
        all_nodes.append(last_node)

        # all_feats.append(stream_feat)
        stream_feat = streams_layer.GetNextFeature()

    # at this point no longer need dataset
    streams_defn = None
    streams_layer = None
    streams_ds = None

    printer.msg("Finding node intersections..")
    tolerance2 = tolerance*tolerance
    filtered_nodes = []
    connected_node_ids = []

    # outer loop on every node
    start_index = 0
    count_nodes = len(all_nodes)
    while start_index < count_nodes-1:
        this_node = all_nodes[start_index]
        start_index += 1
        # skip if it's already been connected
        if this_node.id in connected_node_ids:
            continue
        filtered_nodes.append(this_node)
        # inner loop on every node pair
        for i in range(start_index, count_nodes, 1):
            this_coords = this_node.coords
            that_coords = all_nodes[i].coords
            # check overlap
            distance_x = abs(this_coords[0] - that_coords[0])
            if distance_x <= tolerance:
                distance_y = abs(this_coords[1] - that_coords[1])
                if distance_y <= tolerance:
                    distance = distance_x*distance_x + distance_y*distance_y
                    if distance <= tolerance2:
                        # overlaps, consolidate nodes
                        this_node.segments += all_nodes[i].segments
                        connected_node_ids.append(all_nodes[i].id)

    printer.msg("Finalizing network..")
    # reverse mapping by stream id
    network_map = {}
    for sid in all_stream_ids:
        network_map[sid] = []
    # reassign node ids while we go
    current_id = 1
    for node in filtered_nodes:
        node.id = current_id
        current_id += 1
        for sid in node.segments:
            network_map[sid].append(node.id)

    printer.msg("Saving node network table..")
    if output_table_path:
        with open(output_table_path, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["NODE", "STREAM_IDS"])
            for node in filtered_nodes:
                writer.writerow([node.id, ",".join([str(sid) for sid in node.segments])])
    if output_table_rev_path:
        with open(output_table_rev_path, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["STREAM_ID", "NODES"])
            for sid, nids in network_map.iteritems():
                writer.writerow([sid, ",".join([str(nid) for nid in nids])])

    printer.msg("Saving node shapefile..")
    fields = [
        {'name': "NODE",       'type': int},
        {'name': "STREAM_IDS", 'type': basestring},
        {'name': "END_POINT",  'type': int},
        {'name': "DRAINAGE",   'type': int}
    ]
    node_ds = feature_utils.createFeatureDataset(
        output_node_shp,
        "nodes",
        streams_srs,
        ogr.wkbPoint,
        fields=fields,
        overwrite=True
    )
    node_layer = node_ds.GetLayer()
    defn = node_layer.GetLayerDefn()
    for node in filtered_nodes:
        feat = ogr.Feature(defn)
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(node.coords[0], node.coords[1])
        streams = node.segments
        feat.SetGeometry(pt)
        feat.SetField(fields[0]['name'], node.id)
        feat.SetField(fields[1]['name'], ",".join([str(sid) for sid in streams]))
        feat.SetField(fields[2]['name'], 1 if len(streams) <= 1 else 0)
        feat.SetField(fields[3]['name'], 0)
        node_layer.CreateFeature(feat)
        feat = None
    defn = None
    node_layer = None
    node_ds = None
