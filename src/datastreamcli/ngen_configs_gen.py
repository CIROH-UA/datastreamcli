import geopandas as gpd
import pandas as pd
import argparse
import re, os
import pickle, copy
from pathlib import Path
import datetime
import subprocess
gpd.options.io_engine = "pyogrio"

from ngen.config_gen.file_writer import DefaultFileWriter
from ngen.config_gen.hook_providers import DefaultHookProvider
from ngen.config_gen.generate import generate_configs

from ngen.config_gen.models.cfe import Cfe
from ngen.config_gen.models.pet import Pet

from ngen.config.realization import NgenRealization
from ngen.config.configurations import Routing

LSTM_TEMPLATE = data = {
    "time_step": "",
    "area_sqkm": 0,
    "basin_id": "cat-1",
    "basin_name": "cat-1",
    "elev_mean": 0,
    "initial_state": "zero",
    "lat": None,  
    "lon": None,  
    "slope_mean": 0,
    "train_cfg_file": [
        "/ngen/ngen/extern/lstm/trained_neuralhydrology_models/nh_AORC_hourly_25yr_1210_112435_7/config.yml",
        "/ngen/ngen/extern/lstm/trained_neuralhydrology_models/nh_AORC_hourly_25yr_1210_112435_8/config.yml",
        "/ngen/ngen/extern/lstm/trained_neuralhydrology_models/nh_AORC_hourly_25yr_1210_112435_9/config.yml",
        "/ngen/ngen/extern/lstm/trained_neuralhydrology_models/nh_AORC_hourly_25yr_seq999_seed101_0701_143442/config.yml",
        "/ngen/ngen/extern/lstm/trained_neuralhydrology_models/nh_AORC_hourly_25yr_seq999_seed103_2701_171540/config.yml",
        "/ngen/ngen/extern/lstm/trained_neuralhydrology_models/nh_AORC_hourly_25yr_slope_elev_precip_temp_seq999_seed101_2801_191806/config.yml"
    ],
    "verbose": 0
}

def get_hf(hf_file : str):
    """
    Parameters:
        hf_file : path to hydrofabric file (*.gpkg)

    Returns:
        hf : divide layer of hydrofabric
        layers :  all layers within the hydrofabric file
        attrs : divide attributes (found under different layers)
            v2.1 -> model-attributes
            v2.2 -> divide-attributes
    """

    hf: gpd.GeoDataFrame = gpd.read_file(hf_file, layer="divides") 
    layers = gpd.list_layers(hf_file)
    if "model-attributes" in list(layers.name):
        attrs: pd.DataFrame = gpd.read_file(hf_file,layer="model-attributes")
    elif "divide-attributes" in list(layers.name):
        attrs: pd.DataFrame = gpd.read_file(hf_file,layer="divide-attributes")
    else:
        raise Exception(f"Can't find attributes!")        

    return hf, layers, attrs

def gen_noah_owp_confs_from_pkl(pkl_file,out_dir,start,end):

    if not os.path.exists(out_dir):
        os.system(f"mkdir -p {out_dir}")

    with open(pkl_file, 'rb') as fp:
        nom_dict = pickle.load(fp)

    for jcatch in nom_dict:
        jcatch_str = copy.deepcopy(nom_dict[jcatch])
        for j,jline in enumerate(jcatch_str):
            if "startdate" in jline:
                pattern = r'(startdate\s*=\s*")[0-9]{12}'
                jcatch_str[j] = re.sub(pattern, f"startdate        = \"{start.strftime('%Y%m%d%H%M')}", jline)
            if "enddate" in jline:
                pattern = r'(enddate\s*=\s*")[0-9]{12}'
                jcatch_str[j] =  re.sub(pattern, f"enddate          = \"{end.strftime('%Y%m%d%H%M')}", jline)

        with open(Path(out_dir,f"noah-owp-modular-init-{jcatch}.namelist.input"),"w") as fp:
            fp.writelines(jcatch_str)

def multiprocess_noahowp():
    with open(pkl_file, 'rb') as fp:
        nom_dict = pickle.load(fp)
    catchment_list = list(nom_dict.keys())

    nprocs = os.cpu_count() - 1
    ncatch = len(nom_dict)
    catchment_list_list = []
    gdf_list = []
    nper = ncatch // nprocs
    nleft = ncatch - (nper * nprocs)   
    i = 0
    k = nper
    for j in range(nprocs):
        if j < nleft: k += 1
        catchment_list_list.append(catchment_list[i:k])
        gdf_list.append(gdf[i:k])
        i=k
        k = nper + i
        
    all_proc_confs = {}
    with cf.ProcessPoolExecutor(max_workers=nprocs) as pool:
        for results in pool.map(
        gen_noah_owp_pkl,
        catchment_list_list,
        gdf_list
        ):
            all_proc_confs.update(results)

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

def gen_lstm(hf,attrs,out,real):
    lstm_config_dir = Path(out,'cat_config/LSTM')
    if not Path.exists(lstm_config_dir):
        os.system(f"mkdir -p {lstm_config_dir}")

    lstm_config = copy.copy(LSTM_TEMPLATE)
    interval = real.time.output_interval // 3600
    lstm_config['time_step'] = f"{interval} hour"
    cats = attrs['divide_id']
    ncats = len(cats)
    from pyproj import Transformer
    import yaml
    count = 0
    for x, y in zip(hf.sort_values(by="divide_id").iterrows(),attrs.sort_values(by="divide_id").iterrows()) :    
        count += 1
        j, hf_row = x    
        k, attrs_row =y
        lstm_config_jcat = copy.copy(lstm_config)
        jcat = attrs_row['divide_id']
        source_crs = 'EPSG:5070' 
        target_crs = 'EPSG:4326'
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        x_coord = attrs_row['centroid_x']
        y_coord = attrs_row['centroid_y']
        lon, lat = transformer.transform(x_coord,y_coord)        
        lstm_config_jcat['area_sqkm'] = hf_row['areasqkm']
        lstm_config_jcat['basid_id'] = jcat  
        lstm_config_jcat['basid_name'] = jcat    
        lstm_config_jcat['elev_mean'] = attrs_row['mean.elevation']    
        lstm_config_jcat['lat'] = lat
        lstm_config_jcat['lon'] = lon
        lstm_config_jcat['slope_mean'] = attrs_row['mean.slope']   
        filename = Path(lstm_config_dir, jcat + ".yml")
        with open(filename,"w") as fp:
            yaml.dump(lstm_config_jcat, fp, default_flow_style=False, sort_keys=False)
        perc_comp = 100 * (count/ncats)
        print(f"{perc_comp:.1f}% complete",end='\r')

    return

def gen_petAORcfe(hf,attrs,out,include):
    models = []
    if 'PET' in include:
        models.append(Pet)
    if 'CFE' in include:
        models.append(Cfe)        
    for j, jmodel in enumerate(include):
        hook_provider = DefaultHookProvider(hf=hf, hf_lnk_data=attrs)
        jmodel_out = Path(out,'cat_config',jmodel)
        os.system(f"mkdir -p {jmodel_out}")
        file_writer = DefaultFileWriter(jmodel_out)
        generate_configs(
            hook_providers=hook_provider,
            hook_objects=[models[j]],
            file_writer=file_writer,
        )

# Austin's multiprocess example from chat 3/25
# import concurrent.futures as cf
# from functools import partial
# def generate_configs_multiprocessing(
#     hook_providers: Iterable["HookProvider"],
#     hook_objects: Collection[BuilderVisitableFn],
#     file_writer: FileWriter,
#     pool: cf.ProcessPoolExecutor,
# ):
#     def capture(divide)_id: str, bv: BuilderVisitableFn):
#         bld_vbl = bv()
#         bld_vbl.visit(hook_prov)
#         model = bld_vbl.build()
#         file_writer(divide_id, model)

#     div_hook_obj = DivideIdHookObject()
#     for hook_prov in hook_providers:
#         # retrieve current divide id
#         div_hook_obj.visit(hook_prov)
#         divide_id = div_hook_obj.divide_id()
#         assert divide_id is not None

#         fn = partial(capture, divide_id=divide_id)
#         pool.map(fn, hook_objects)

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
        "--pkl_file",
        dest="pkl_file", 
        type=str,
        help="Path to the noahowp pkl", 
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

    global start,end
    serialized_realization = NgenRealization.parse_file(args.realization)
    start = serialized_realization.time.start_time
    end   = serialized_realization.time.end_time    
    max_loop_size = (end - start + datetime.timedelta(hours=1)).total_seconds() / (serialized_realization.time.output_interval)
    models = []
    ii_cfe_or_pet = False
    model_names = []
    for jform in serialized_realization.global_config.formulations:
        for jmod in jform.params.modules:
            model_names.append(jmod.params.model_name)

    geo_file_path = os.path.join("./config",os.path.basename(args.hf_file))

    dir_dict = {"CFE":"CFE",
                "PET":"PET",
                "NoahOWP":"NOAH-OWP-M",
                "SLOTH":"",
                "bmi_rust":"lstm"}

    ignore = []
    for jmodel in model_names:
        config_path = Path(args.outdir,"cat_config",dir_dict[jmodel])
        if config_path.exists(): ignore.append(jmodel)
    routing_path = Path(args.outdir,"troute.yaml")
    if routing_path.exists(): ignore.append("routing")       

    hf, layers, attrs = get_hf(args.hf_file)

    if "NoahOWP" in model_names:
        if "NoahOWP" in ignore:
            print(f'ignoring NoahOWP')
        else:
            if "pkl_file" in args:
                print(f'Generating NoahOWP configs from pickle',flush = True)
                global noah_dir,pkl_file
                pkl_file = args.pkl_file
                noah_dir = Path(args.outdir,'cat_config','NOAH-OWP-M')
                os.system(f'mkdir -p {noah_dir}')
                gen_noah_owp_confs_from_pkl(args.pkl_file, noah_dir, start, end)
            else:
                raise Exception(f"Generating NoahOWP configs manually not implemented, create pkl.")            

    if "CFE" in model_names: 
        if "CFE" in ignore:
            print(f'ignoring CFE')
        else:
            print(f'Generating CFE configs from pydantic models',flush = True)
            gen_petAORcfe(hf,attrs,args.outdir,["CFE"])

    if "PET" in model_names: 
        if "PET" in ignore:
            print(f'ignoring PET')
        else:
            print(f'Generating PET configs from pydantic models',flush = True)
            gen_petAORcfe(hf,attrs,args.outdir,["PET"])

    if "bmi_rust" in model_names:
        if "bmi_rust" in ignore:
            print(f'ignoring LSTM')
        else:
            print(f'Generating LSTM configs from pydantic models',flush = True)
            gen_lstm(hf,attrs,args.outdir,serialized_realization)        

    if "bmi_rust" in model_names:
        if "bmi_rust" in ignore:
            print(f'ignoring LSTM')
        else:
            print(f'Generating LSTM configs from pydantic models',flush = True)
            gen_LSTM(args.hf_file,args.outdir)        


    globals = [x[0] for x in serialized_realization]
    if serialized_realization.routing is not None:
        if "routing" in ignore:
            print(f'ignoring routing')
        else:
            print(f'Generating t-route config from template',flush = True)
            generate_troute_conf(args.outdir,start,max_loop_size,geo_file_path) 

    print(f'Done!',flush = True)
