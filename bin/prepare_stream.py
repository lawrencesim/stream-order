import ogr
from bin.PassPrint import PassPrint
from common import feature_utils


printer = None


def prepare_stream(input_stream_dataset, copy_stream_dataset, stream_id_column, tolerance):
    ogr.UseExceptions()

    global printer
    if not printer:
        printer = PassPrint()

    try:
        printer.msg("Opening dataset..")
        input_streams_ds = feature_utils.getFeatureDataset(input_stream_dataset)
        input_streams_layer = input_streams_ds.GetLayer()

        streams_ds = feature_utils.copyFeatureDatasetAsEmpty(input_streams_ds, output_path=copy_stream_dataset, overwrite=True, new_geom_type=ogr.wkbLineString)
        streams_layer = streams_ds.GetLayer()
        streams_defn = streams_layer.GetLayerDefn()

        # check for stream id column
        printer.msg("Checking attributes..")
        input_fields = feature_utils.getFields(input_streams_layer)
        input_sid_field = None
        for field in input_fields:
            if field['name'].lower() == stream_id_column.lower():
                input_sid_field = field
                break
        if not input_sid_field:
            printer.warn("  stream ID column not found, creating..")
            feature_utils.createFieldDefinition(stream_id_column, int)
        fields = feature_utils.getFields(streams_layer)
        field_sid = None
        for f in fields:
            if f['name'] == stream_id_column:
                field_sid = f
                break

        printer.msg("Copying features..")
        _copy_feature(input_streams_layer, streams_layer, streams_defn, input_fields, input_sid_field, stream_id_column)

        input_streams_layer = None
        input_streams_ds = None

        # printer.msg("Snapping endpoints..")
        # new_features = _snap_endpoints(streams_layer, streams_defn, fields)
        # if len(new_features):
        #     printer.msg("  adding split features..")
        #     streams_ds, streams_layer, streams_defn = _replace_and_update(streams_ds, copy_stream_dataset, new_features)

        printer.msg("Checking intersections..")
        new_features = _split_intersections(streams_layer, streams_defn, fields, field_sid)
        if len(new_features):
            printer.msg("  adding split features..")
            streams_ds, streams_layer, streams_defn = _replace_and_update(streams_ds, copy_stream_dataset, new_features)

        printer.msg("Assigning new stream IDs..")
        _assign_stream_ids(streams_layer, field_sid)

    except Exception:
        input_streams_layer = None
        input_streams_ds = None
        streams_layer = None
        streams_defn = None
        streams_ds = None
        raise


def _replace_and_update(dataset, path, new_features):
    layer = dataset.GetLayer()
    for feat in new_features:
        layer.CreateFeature(feat)
    feat = None
    new_features = None
    # close and reopen to commit changes
    layer = None
    dataset = None
    dataset = feature_utils.getFeatureDataset(path, write=1)
    layer = dataset.GetLayer()
    defn = layer.GetLayerDefn()
    return dataset, layer, defn


def _copy_feature(input_streams_layer, streams_layer, streams_defn, input_fields, input_sid_field, stream_id_column):
    num_streams = input_streams_layer.GetFeatureCount()
    for i in range(num_streams):
        feat = input_streams_layer.GetFeature(i)
        geom = feat.GetGeometryRef()
        geom_type = geom.GetGeometryType()

        # check if multiline string in two ways
        coords = None
        if geom_type == ogr.wkbMultiLineString or geom_type == ogr.wkbMultiLineString25D or geom_type == ogr.wkbMultiLineStringM or geom_type == ogr.wkbMultiLineStringM:
            coords = []
            for g in range(geom.GetGeometryCount()):
                line = geom.GetGeometryRef(g)
                coords.append(line.GetPoints())
            coords = geom.GetPoints()
        elif geom_type != ogr.wkbLineString and geom_type != ogr.wkbLineString25D and geom_type == ogr.wkbLineStringM and geom_type == ogr.wkbLineStringM:
            raise Exception("Stream network must be LineString (or MultiLineString which can be converted)")
        else:
            coords = [geom.GetPoints()]

        # copy
        first = True
        for line in coords:
            copy_feat = ogr.Feature(streams_defn)
            copy_geom = ogr.Geometry(ogr.wkbLineString)
            for point in line:
                copy_geom.AddPoint(point[0], point[1])
            copy_feat.SetGeometry(copy_geom)
            if first:
                for field in input_fields:
                    copy_feat.SetField(field['name'], feat.GetField(field['index']))
            if not first or not input_sid_field:
                copy_feat.SetField(stream_id_column, -1)
            streams_layer.CreateFeature(copy_feat)
            copy_feat = None
            first = False


def _snap_endpoints(streams_layer, streams_defn, fields, tolerance):
    tolerance2 = tolerance*tolerance
    num_streams = streams_layer.GetFeatureCount()

    # first create segments for all geometries
    all_segments = []
    for i in range(num_streams):
        this_feat = streams_layer.GetFeature(i)
        this_geom = this_feat.GetGeometryRef()
        # get "this" coordinates
        this_line = this_geom.GetPoints()
        # create segments
        all_segments.append([])
        for c in range(1, this_line.length):
            segment_start = this_line[c-1]
            segment_end = this_line[c]
            if segment_start[0] > segment_end[1]:
                swap = segment_start
                segment_start = segment_end
                segment_end = swap
            if segment_start[1] == segment_end[1]:
                slope = float("inf")
            else:
                slope = (segment_end[1] - segment_start[1]) / (segment_end[0] - segment_start[0])
            positive_slope = slope == float("inf") or slope >= 0
            all_segments[i].append({
                'x0': segment_start[0],
                'x1': segment_end[0],
                'y0': segment_start[1],
                'y1': segment_end[1],
                'ymin': segment_start[1] if positive_slope else segment_end[1],
                'ymax': segment_end[1] if positive_slope else segment_start[1],
                'slope': slope,
                'yintercept': segment_start[1] - slope*segment_start[0]
            })
        this_line = None
        this_geom = None
        this_feat = None

    new_features = []
    remove_features = []
    # loop features
    for i in range(num_streams):
        this_feat = streams_layer.GetFeature(i)
        this_geom = this_feat.GetGeometryRef()
        points = this_geom.GetPoints()
        endpoints = [points[0], points[-1]]

        # way this works is all possible snap points and distances are kept and closest is picked
        snapped_ends = [[], []]

        # loop against segments of other features
        for j in range(i+1, num_streams):
            for segment in all_segments[j]:

                # check each endpoint distance from segment
                for e in range(2):
                    endpoint = endpoints[e]
                    nearest_pt = None
                    # quick check if outside x-range
                    distance_x = 0
                    if endpoint[0] < segment['x0']:
                        distance_x = abs(segment['x0'] - endpoint[0])
                    elif endpoint[1] > segment['x1']:
                        distance_x = abs(segment['x1'] - endpoint[0])
                    if distance_x > tolerance:
                        continue
                    # same with -yrange
                    distance_y = 0
                    if endpoint[1] < segment['ymin']:
                        distance_y = abs(segment['ymin'] - endpoint[1])
                    elif endpoint[1] > segment['ymax']:
                        distance_y = abs(segment['ymax'] - endpoint[1])
                    if distance_y > tolerance:
                        continue
                    # vertical line
                    if slope == float("inf"):
                        distance_x2 = (segment['x0'] - endpoint[0])**2
                        if endpoint[1] < segment['y0']:
                            nearest_pt = segment_start
                            distance2 = distance_x2 + (segment['y0'] - endpoint[1])**2
                        elif endpoint[1] > segment['y1']:
                            nearest_pt = segment_end
                            distance2 = distance_x2 + (segment['y1'] - endpoint[1])**2
                        else:
                            nearest_pt = (segment['x0'], endpoint[1])
                            distance2 = distance_x2
                    # flat line
                    elif slope == 0:
                        distance_y2 = (segment['y0'] - endpoint[1])**2
                        if endpoint[0] < segment['x0']:
                            nearest_pt = segment_start
                            distance2 = distance_y2 + (segment['x0'] - endpoint[0])**2
                        elif endpoint[0] > segment['x1']:
                            nearest_pt = segment_end
                            distance2 = distance_y2 + (segment['x1'] - endpoint[0])**2
                        else:
                            nearest_pt = (endpoint[0], segment['y0'])
                            distance2 = distance_y2
                    else:
                        # solve for nearest point on segment
                        critp_x = segment['slope']*(endpoint[1] - segment['slope']*endpoint[0] - segment['yintercept'])
                        critp_x /= (1 + segment['slope']**2)
                        critp_x += endpoint[0]
                        distance_x2 = (critp_x - endpoint[0])**2
                        if distance_x2 < tolerance2:
                            critp_y = segment['slope']*critp_x + segment['yintercept']
                            distance_y2 = (critp_y - endpoint[1])**2
                            if distance_y2 < tolerance2:
                                nearest_pt = (critp_x, critp_y)
                                distance2 = distance_x2 + distance_y2
                    # add to list
                    if nearest_pt and distance2 <= tolerance2:
                        snapped_ends[e].append([nearest_pt, distance2])
        # end of inner loops

        # check list of snapped ends for closest
        best_snap = [None, None]
        for p in range(2):
            for snap in snapped_ends[p]:
                # actual intersect exists, then don't snap
                if snap[1] == 0:
                    best_snap[p] = False
                    break
                if not best_snap[p] or snap[1] < best_snap[p][1]:
                    best_snap[p] = snap
        snapped_ends = None

        # add snap point to existing line
        changed = False
        for p in range(2):
            if best_snap[p]:
                changed = True
                if p == 0:
                    points = [best_snap[p][0]] + points
                else:
                    points += [best_snap[p][0]]
        best_snap = None

        # mark as new feature and feature to remove
        if changed:
            remove_features.append(this_feat)
            new_geom = ogr.Geometry(ogr.wkbLineString)
            for p in points:
                new_geom.AddPoint(p[0], p[1])
            new_feat = ogr.Feature(streams_defn)
            new_feat.SetGeometry(new_geom)
            new_geom = None
            for field in fields:
                new_feat.SetField(field['name'], feature_utils.getFieldValue(this_feat, field))
            new_features.append(new_feat)
            new_feat = None

        this_geom = None
        this_feat = None

    if len(remove_features):
        printer.msg("  deleting split features..")
        for feat in remove_features:
            fid = feat.GetFID()
            streams_layer.DeleteFeature(fid)
            feat = None
        remove_features = None

    return new_features


def _split_intersections(streams_layer, streams_defn, fields, field_sid):
    global printer
    num_streams = streams_layer.GetFeatureCount()
    new_features = []
    remove_features = []
    removed_indices = []
    for i in range(num_streams):
        this_feat = streams_layer.GetFeature(i)
        this_geom = this_feat.GetGeometryRef()
        this_env = this_geom.GetEnvelope()
        this_id = feature_utils.getFieldValue(this_feat, field_sid)

        for j in range(i+1, num_streams):
            that_feat = streams_layer.GetFeature(j)
            that_geom = that_feat.GetGeometryRef()
            that_env = that_geom.GetEnvelope()

            # check bounds
            check_intersection = True
            if that_env[0] > this_env[1] or that_env[1] < this_env[0]:
                check_intersection = False
            if check_intersection and (that_env[2] > this_env[3] or that_env[3] < this_env[2]):
                check_intersection = False

            # check intersection
            has_intersection = False
            if check_intersection:
                intersection = this_geom.Intersection(that_geom)
                if not intersection.IsEmpty():
                    has_intersection = True
                    igtype = intersection.GetGeometryType()
                    if igtype == ogr.wkbPoint:
                        intersection_points = intersection.GetPoints()
                    elif igtype == ogr.wkbMultiPoint:
                        intersection_points = []
                        for g in range(intersection.GetGeometryCount()):
                            point = intersection.GetGeometryRef(g)
                            intersection_points.append(point.GetPoints()[0])
                            point = None
                    else:
                        # don't currently handle line intersections
                        raise Exception("Unable to handle intersection type {0}".format(igtype))
                    if not intersection_points or not len(intersection_points):
                        has_intersection = False
            # split out new line segments
            if has_intersection:
                this_split_feats = _split_feature(streams_layer, streams_defn, fields, field_sid, this_feat, intersection_points)
                if len(this_split_feats):
                    printer.msg("  split segment={0}".format(this_id))
                    if this_id not in removed_indices:
                        removed_indices.append(this_id)
                        remove_features.append(this_feat)
                    new_features += this_split_feats
                that_split_feats = _split_feature(streams_layer, streams_defn, fields, field_sid, that_feat, intersection_points)
                if len(that_split_feats):
                    that_id = feature_utils.getFieldValue(that_feat, field_sid)
                    printer.msg("  split segment={0}".format(that_id))
                    if that_id not in remove_features:
                        removed_indices.append(that_id)
                        remove_features.append(that_feat)
                    new_features += that_split_feats
                add_new_feats = None

            intersection = None
            that_env = None
            that_geom = None
            that_feat = None

        this_env = None
        this_geom = None
        this_feat = None

    if len(remove_features):
        printer.msg("  deleting split features..")
        for feat in remove_features:
            fid = feat.GetFID()
            streams_layer.DeleteFeature(fid)
            feat = None
        remove_features = None

    return new_features


def _split_feature(layer, defn, fields, id_column, feature, intersection_points):
    geom = feature.GetGeometryRef()
    coords = geom.GetPoints()

    # split current geometry by intersection points
    new_line_coords = []
    running_coords = []
    for point in coords:
        running_coords.append(point)
        for ipoint in intersection_points:
            if point[0] == ipoint[0] and point[1] == ipoint[1]:
                if len(running_coords) >= 2:
                    new_line_coords.append(running_coords)
                    running_coords = [running_coords[-1]]
                break
    if len(running_coords) >= 2:
        new_line_coords.append(running_coords)

    # create new features based on splits
    new_features = []
    if len(new_line_coords) >= 2:
        # create new features (only first keeps id)
        first = True
        for new_coords in new_line_coords:
            new_geom = ogr.Geometry(ogr.wkbLineString)
            for point in new_coords:
                new_geom.AddPoint(point[0], point[1])
            new_feat = ogr.Feature(defn)
            new_feat.SetGeometry(new_geom)
            new_geom = None
            # copy every attribute but overwrite ID as unknown
            for field in fields:
                new_feat.SetField(field['name'], feature_utils.getFieldValue(feature, field))
            if not first:
                new_feat.SetField(id_column['name'], -1)
            else:
                first = False
            new_features.append(new_feat)

    coords = None
    geom = None

    return new_features


def _assign_stream_ids(streams_layer, field_sid):
    existing_ids = []
    num_streams = streams_layer.GetFeatureCount()
    for i in range(num_streams):
        feat = streams_layer.GetFeature(i)
        sid = feature_utils.getFieldValue(feat, field_sid)
        if sid > 0:
            if sid in existing_ids:
                feat.SetField(field_sid['name'], -1)
                streams_layer.SetFeature(feat)
            else:
                existing_ids.append(sid)

    existing_ids.sort()
    next_id = existing_ids[-1]

    for i in range(num_streams):
        feat = streams_layer.GetFeature(i)
        sid = feature_utils.getFieldValue(feat, field_sid)
        if sid <= 0:
            next_id += 1
            feat.SetField(field_sid['name'], next_id)
            streams_layer.SetFeature(feat)
