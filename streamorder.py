import bin.prepare_stream
import bin.get_node_network
import bin.calculate_flow
import bin.complete_braided_streams
import bin.calculate_stream_order


printer = None


def prepare_stream(input_stream_dataset, copy_stream_dataset, stream_id_column, tolerance=1.0):
    global printer
    module = bin.prepare_stream
    module.printer = printer
    return module.prepare_stream(input_stream_dataset, copy_stream_dataset, stream_id_column, tolerance)


def get_node_network(stream_dataset, stream_id_column, output_node_shp, output_table_path, output_table_rev_path=None,
                     tolerance=1):
    global printer
    module = bin.get_node_network
    module.printer = printer
    return module.get_node_network(stream_dataset, stream_id_column, output_node_shp, output_table_path,
                                   output_table_rev_path, tolerance)


def calculate_flow(stream_dataset, stream_id_column, node_dataset_or_table, drainage_node_ids=None,
                   from_node_column="FROM_NODE", to_node_column="TO_NODE", braided_column="BRAIDED",
                   output_flow_table_path=None):
    global printer
    module = bin.calculate_flow
    module.printer = printer
    return module.calculate_flow(stream_dataset, stream_id_column, node_dataset_or_table, drainage_node_ids,
                                 from_node_column, to_node_column, braided_column, output_flow_table_path)


def complete_braided_streams(stream_dataset, stream_id_column, node_dataset_or_table, from_node_column="FROM_NODE",
                             to_node_column="TO_NODE", braided_column="BRAIDED", elevation_dataset=None):
    global printer
    module = bin.complete_braided_streams
    module.printer = printer
    return module.complete_braided_streams(stream_dataset, stream_id_column, node_dataset_or_table, from_node_column,
                                           to_node_column, braided_column, elevation_dataset)


def calculate_stream_order(stream_dataset, stream_id_column, from_node_column="FROM_NODE", to_node_column="TO_NODE",
                           braided_column="BRAIDED", stream_order_column="STRAHLER", output_table_path=None):
    global printer
    module = bin.calculate_stream_order
    module.printer = printer
    return module.calculate_stream_order(stream_dataset, stream_id_column, from_node_column, to_node_column,
                                         braided_column, stream_order_column, output_table_path)
