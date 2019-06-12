import os
import shutil
import streamorder
from bin.PassPrint import PassPrint


# streams shapefile, will be copied
streams_shp = r"C:\Users\LawrenceS\Documents\ProjectsPython\stream-order\test\NHDFlowline_CAalbers_clean_modified.shp"
# output directory
out_dir = r"C:\Users\LawrenceS\Documents\ProjectsPython\stream-order\test\test_prepare"
# column names -- aside from stream ID, can be optional and defaults will be used
stream_id_col           = "STREAM_ID"
from_node_col           = "FROM_NODE"
to_node_col             = "TO_NODE"
braided_col             = "BRAIDED"
stream_order_col        = "STRAHLER"
# outputs -- aside from Node shapefile, optional and can be Null
out_node_shp            = "nodes.shp"
out_network_table       = "network.csv"
out_network_rev_table   = "network_rev.csv"
out_flow_table          = "flow.csv"
out_stream_order_table  = "streamorder.csv"
# drainage node IDs must be manually supplied before doing calculate flow
drainage_node_ids       = [1899]  #[2, 58, 85, 91, 425, 689, 767, 772, 1099, 1161, 1303]
# optional DEM for more accurate processing of braided streams
elevation_dataset       = r"C:\Users\LawrenceS\Documents\ProjectsPython\stream-order\test\ned10m.img"
# processes to run
do = {
    'prepare_stream':           True,
    'get_node_network':         False,
    'calculate_flow':           False,
    'complete_braided_streams': False,
    'calculate_stream_order':   False,
}


if __name__ == '__main__':

    printer = PassPrint()
    streamorder.printer = printer

    printer.msg("Preparing..")
    printer.increase_indent()

    out_network_table      = os.path.join(out_dir, out_network_table) if out_network_table else None
    out_network_rev_table  = os.path.join(out_dir, out_network_rev_table) if out_network_rev_table else None
    out_node_shp           = os.path.join(out_dir, out_node_shp) if out_node_shp else None
    out_flow_table         = os.path.join(out_dir, out_flow_table) if out_flow_table else None
    out_stream_order_table = os.path.join(out_dir, out_stream_order_table) if out_stream_order_table else None

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # copy shapefile
    working_streams_shp = os.path.join(out_dir, "streams.shp")
    if not os.path.exists(working_streams_shp):
        printer.msg("Copying streams shapefile..")
        streams_shp_dir = os.path.dirname(streams_shp)
        streams_shp_name = os.path.basename(streams_shp)
        streams_shp_basename = os.path.splitext(streams_shp_name)[0].lower()
        for filename in os.listdir(streams_shp_dir):
            namesplit = os.path.splitext(filename)
            if namesplit[0].lower() == streams_shp_basename:
                copy_path = os.path.join(out_dir, "streams"+namesplit[1])
                if os.path.exists(copy_path):
                    os.remove(copy_path)
                shutil.copyfile(
                    os.path.join(streams_shp_dir, filename),
                    copy_path
                )
    printer.msg("")

    if do['prepare_stream']:
        printer.decrease_indent()
        printer.msg("Preparing Streams Data..")
        printer.increase_indent()
        # rename working shapefile first
        streams_shp_dir = os.path.dirname(working_streams_shp)
        streams_shp_name = os.path.basename(working_streams_shp)
        for filename in os.listdir(streams_shp_dir):
            namesplit = os.path.splitext(filename)
            if namesplit[0].lower() == "streams":
                copy_path = os.path.join(out_dir, "streams_raw"+namesplit[1])
                if os.path.exists(copy_path):
                    os.remove(copy_path)
                os.rename(
                    os.path.join(streams_shp_dir, filename),
                    copy_path
                )
        # run process
        try:
            streamorder.prepare_stream(
                input_stream_dataset=os.path.join(out_dir, "streams_raw.shp"),
                copy_stream_dataset=working_streams_shp,
                stream_id_column=stream_id_col
            )
        except Exception:
            # delete on error
            for filename in os.listdir(streams_shp_dir):
                namesplit = os.path.splitext(filename)
                if namesplit[0].lower() == "streams" or namesplit[0].lower() == "streams_raw":
                    os.remove(os.path.join(out_dir, filename))
            raise
        printer.msg("")

    if do['get_node_network']:
        printer.decrease_indent()
        printer.msg("Creating Node Network..")
        printer.increase_indent()
        streamorder.get_node_network(
            stream_dataset=working_streams_shp,
            stream_id_column=stream_id_col,
            output_table_path=out_network_table,
            output_table_rev_path=out_network_rev_table,
            output_node_shp=out_node_shp
        )
        printer.msg("")

    if do['calculate_flow']:
        printer.decrease_indent()
        printer.msg("Calculating Network Flow..")
        printer.increase_indent()
        streamorder.calculate_flow(
            stream_dataset=working_streams_shp,
            stream_id_column=stream_id_col,
            node_dataset_or_table=out_node_shp,
            drainage_node_ids=drainage_node_ids,
            from_node_column=from_node_col,
            to_node_column=to_node_col,
            braided_column=braided_col,
            output_flow_table_path=out_flow_table
        )
        printer.msg("")

    if do['complete_braided_streams']:
        printer.decrease_indent()
        printer.msg("Completing Braided Streams..")
        printer.increase_indent()
        streamorder.complete_braided_streams(
            stream_dataset=working_streams_shp,
            stream_id_column=stream_id_col,
            node_dataset_or_table=out_node_shp,
            from_node_column=from_node_col,
            to_node_column=to_node_col,
            braided_column=braided_col,
            elevation_dataset=elevation_dataset
        )
        printer.msg("")

    if do['calculate_stream_order']:
        printer.decrease_indent()
        printer.msg("Calculating Stream Order..")
        printer.increase_indent()
        streamorder.calculate_stream_order(
            stream_dataset=working_streams_shp,
            stream_id_column=stream_id_col,
            from_node_column=from_node_col,
            to_node_column=to_node_col,
            braided_column=braided_col,
            stream_order_column=stream_order_col,
            output_table_path=out_stream_order_table
        )
        printer.msg("")

    printer.decrease_indent()
    printer.msg("Done.")
