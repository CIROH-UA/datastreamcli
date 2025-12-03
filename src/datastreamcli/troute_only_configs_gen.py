from pathlib import Path
import re
import argparse
import datetime
import os

from ngen.config.realization import NgenRealization

def generate_troute_conf(out_dir,start,max_loop_size,geo_file_path):

    template = Path(__file__).parent.parent.parent/"configs/ngen/troute.yaml"

    with open(template,'r') as fp:
        conf_template = fp.readlines()

    for j,jline in enumerate(conf_template):
        if "qts_subdivisions" in jline:
            qts_subdivisions = int(jline.strip().split(': ')[-1])

    nts = max_loop_size * qts_subdivisions

    troute_conf_str = conf_template
    for j,jline in enumerate(conf_template):
        if "start_datetime" in jline:
            pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
            troute_conf_str[j] = re.sub(pattern, start.strftime('%Y-%m-%d %H:%M:%S'), jline)

        pattern = r'^\s*max_loop_size\s*:\s*\d+\.\d+'
        if re.search(pattern,jline):
            troute_conf_str[j] = re.sub(pattern,  f"    max_loop_size: {max_loop_size}      ", jline)

        pattern = r'^\s*nts\s*:\s*\d+\.\d+'
        if re.search(pattern,jline):
            troute_conf_str[j] = re.sub(pattern,  f"    nts: {nts}      ", jline)

        pattern = r'(geo_file_path:).*'
        if re.search(pattern,jline):
            troute_conf_str[j] = re.sub(pattern,  f'\\1 {geo_file_path}', jline)

    with open(Path(out_dir,"troute.yaml"),'w') as fp:
        fp.writelines(troute_conf_str)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hf_file",
        dest="hf_file",
        type=str,
        help="Path to the .gpkg",
        required=False
    )
    parser.add_argument(
        "--outdir",
        dest="outdir",
        type=str,
        help="Path to write ngen configs",
        required=False
    )
    parser.add_argument(
        "--realization",
        dest="realization",
        type=str,
        help="Path to the ngen realization",
        required=False
    )

    args = parser.parse_args()

    serialized_realization = NgenRealization.parse_file(args.realization)
    start = serialized_realization.time.start_time
    end   = serialized_realization.time.end_time
    max_loop_size = (end - start + datetime.timedelta(hours=1)).total_seconds() / (serialized_realization.time.output_interval)
    geo_file_path = os.path.join("./config",os.path.basename(args.hf_file))

    print('Generating t-route config from template',flush = True)
    generate_troute_conf(args.outdir,start,max_loop_size,geo_file_path)

    print('Done!',flush = True)
