import csv
import common.feature_utils as feature_utils
from bin.network.Node import Node
from bin.network.Segment import Segment


def _check_srs_units(dataset=None, layer=None, srs=None):
    if not srs:
        if not layer:
            dataset = feature_utils.getFeatureDataset(dataset)
        layer = dataset.GetLayer()
        srs = layer.GetSpatialRef()
    units = srs.GetLinearUnitsName().lower()
    if units not in ["foot", "feet", "meter", "meters", "metre", "metres"]:
        raise Exception("Dataset units ({0}) not recognized as valid linear unit.".format(units))


def _get_segments(stream_dataset, stream_id_column, nodes=None, from_node_column=None, to_node_column=None,
                  braided_column=None):
    stream_ds = feature_utils.getFeatureDataset(stream_dataset)
    stream_layer = stream_ds.GetLayer()
    stream_defn = stream_layer.GetLayerDefn()

    f_index = stream_defn.GetFieldIndex(stream_id_column)
    if f_index < 0:
        raise Exception("Could not find column ({0})".format(stream_id_column))
    stream_id_field = stream_defn.GetFieldDefn(f_index)
    if from_node_column:
        f_index = stream_defn.GetFieldIndex(from_node_column)
        if f_index < 0:
            return Exception("Could not find column ({0})".format(from_node_column))
        from_node_column = stream_defn.GetFieldDefn(f_index)
    if to_node_column:
        f_index = stream_defn.GetFieldIndex(to_node_column)
        if f_index < 0:
            return Exception("Could not find column ({0})".format(to_node_column))
        to_node_column = stream_defn.GetFieldDefn(f_index)
    if braided_column:
        f_index = stream_defn.GetFieldIndex(braided_column)
        if f_index < 0:
            return Exception("Could not find column ({0})".format(braided_column))
        braided_column = stream_defn.GetFieldDefn(f_index)

    segments = []
    stream_feat = stream_layer.GetNextFeature()
    while stream_feat:
        sid = feature_utils.getFieldValue(stream_feat, stream_id_field)
        if sid or sid == 0:
            segment = Segment(sid)

            if braided_column:
                segment.braided = feature_utils.getFieldValue(stream_feat, braided_column) > 0
            if from_node_column and to_node_column:
                from_node_id = feature_utils.getFieldValue(stream_feat, from_node_column)
                to_node_id = feature_utils.getFieldValue(stream_feat, to_node_column)
                if not nodes:
                    segment.from_node = from_node_id
                    segment.to_node = to_node_id
                else:
                    from_node = None
                    to_node = None
                    if from_node_id > 0 or to_node_id > 0:
                        for node in nodes:
                            if not from_node and node.id == from_node_id:
                                from_node = node
                            if not to_node and node.id == to_node_id:
                                to_node = node
                            if (from_node or from_node_id <= 0) and (to_node or to_node_id <= 0):
                                break
                    if (from_node_id > 0 and not from_node) or (to_node_id > 0 and not to_node):
                        raise Exception("Could not find nodes ({1}, {2}) for segment={0}".format(sid, from_node_id, to_node_id))
                    if from_node:
                        for s in range(len(from_node.segments)):
                            if from_node.segments[s] == sid:
                                from_node.segments[s] = segment
                                break
                        segment.from_node = from_node
                    if to_node:
                        for s in range(len(to_node.segments)):
                            if to_node.segments[s] == sid:
                                to_node.segments[s] = segment
                                break
                        segment.to_node = to_node

            segments.append(segment)
            stream_feat = stream_layer.GetNextFeature()

    stream_defn = None
    stream_layer = None
    stream_ds = None

    return segments


def _parse_nodes(node_dataset_or_table, drainage_node_ids=None, require_drainage=False, get_coords=False):
    nodes = []
    drainage_nodes = []

    if not node_dataset_or_table.endswith(".shp"):
        # if not shapefile, read as csv table
        # but first require drainage node IDs
        if require_drainage and (not drainage_node_ids or not len(drainage_node_ids)):
            raise Exception("If node network table provided, drainage node ids must be manually supplied")
        # also can't get coords from table output
        if get_coords:
            raise Exception("Coordinates cannot be pulled from node network table")
        with open(node_dataset_or_table, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            first = True
            for row in reader:
                if first:
                    first = False
                elif row[0]:
                    node = Node(int(row[0]))
                    node.segments = [int(sid) for sid in row[1].split(",")]
                    nodes.append(node)

    else:
        node_ds = feature_utils.getFeatureDataset(node_dataset_or_table)
        node_layer = node_ds.GetLayer()

        fields = [
            {'name': "NODE",       'type': int},
            {'name': "STREAM_IDS", 'type': basestring},
            {'name': "END_POINT",  'type': int},
            {'name': "DRAINAGE",   'type': int}
        ]

        node_feat = node_layer.GetNextFeature()
        while node_feat:
            node = Node(feature_utils.getFieldValue(node_feat, fields[0]))

            streamids = feature_utils.getFieldValue(node_feat, fields[1]).split(",")
            node.segments = [int(sid) for sid in streamids]
            nodes.append(node)

            if get_coords:
                geom = node_feat.GetGeometryRef()
                node.coords = geom.GetPoint()

            if feature_utils.getFieldValue(node_feat, fields[3]) > 0:
                drainage_nodes.append(node)

            node_feat = node_layer.GetNextFeature()

        node_feat = None
        node_layer = None
        node_ds = None

    # manually supplied drainage node ids overwrites found
    if require_drainage and drainage_node_ids and len(drainage_node_ids):
        drainage_nodes = []
        for node in nodes:
            if node.id in drainage_node_ids:
                drainage_nodes.append(node)
        if len(drainage_nodes) != len(drainage_node_ids):
            raise Exception("Could not match all drainage node IDs given")

    return nodes, drainage_nodes
