import pandas as pd
import typer
import json
import os
import numpy as np
from typing_extensions import Annotated


def main(
    to_process: Annotated[str, typer.Option(help="Path to the JSON responses folder.")],
    output_dir: Annotated[str, typer.Option(help="The folder where the final transformed_data.csv will be stored.")]
    ):
    
    if os.path.exists(to_process):
        output_file = os.path.join(output_dir, 'transformed_data.csv')
        
        unit_list = []
        trip_id_list = []
        toll_loc_id_start_list = []
        toll_loc_id_end_list = []
        toll_loc_name_start_list = []
        toll_loc_name_end_list = []
        toll_system_type_list = []
        entry_time_list = []
        exit_time_list = []
        tag_cost_list = []
        cash_cost_list = []
        license_plate_cost_list = []
        
        for file in os.listdir(to_process):
            if file.endswith('.json'):
                
                trip_unit_id = file.split('/')[-1]
                unit_list.append(trip_unit_id.split('_')[0])
                trip_id_list.append(trip_unit_id.replace('.json',''))
                
                file_path = os.path.join(to_process, file)
                with open(file_path, 'r') as json_file:
                    jdata = json.load(json_file)
                
                tolls_dict = jdata['route'].get('tolls', None)
                if tolls_dict and len(tolls_dict)>0:
                    toll_start = tolls_dict[0].get('start', None)
                    if toll_start:
                        toll_loc_id_start_list.append(toll_start.get('id', None))
                        toll_loc_name_start_list.append(toll_start.get('name', None))
                        if 'arrival' in toll_start.keys():
                            entry_time_list.append(toll_start['arrival'].get('time', None))
                        else:
                            entry_time_list.append(None)
                    else:
                        toll_loc_id_start_list.append(None)
                        toll_loc_name_start_list.append(None)
                        entry_time_list.append(None)

                    toll_end = tolls_dict[0].get('end', None)
                    if toll_end:
                        toll_loc_id_end_list.append(toll_end.get('id', None))
                        toll_loc_name_end_list.append(toll_end.get('name', None))
                        if 'arrival' in toll_end.keys():
                            exit_time_list.append(toll_end['arrival'].get('time', None))
                        else:
                            exit_time_list.append(None)
                    else:
                        toll_loc_id_end_list.append(None)
                        toll_loc_name_end_list.append(None)
                        exit_time_list.append(None)

                    toll_system_type_list.append(tolls_dict[0].get('type', None))
                    tag_cost_list.append(tolls_dict[0].get('tagCost', None))
                    cash_cost_list.append(tolls_dict[0].get('cashCost', None))
                    license_plate_cost_list.append(tolls_dict[0].get('licensePlateCost', None))
                    
                else:
                    toll_loc_id_start_list.append(None)
                    toll_loc_name_start_list.append(None)
                    entry_time_list.append(None)  
                    toll_loc_id_end_list.append(None)
                    toll_loc_name_end_list.append(None)
                    exit_time_list.append(None)
                    toll_system_type_list.append(None)
                    tag_cost_list.append(None)
                    cash_cost_list.append(None)
                    license_plate_cost_list.append(None)

        extracted_df = pd.DataFrame(dict(
            unit=unit_list,
            trip_id = trip_id_list,
            toll_loc_id_start=toll_loc_id_start_list,
            toll_loc_id_end=toll_loc_id_end_list,
            toll_loc_name_start=toll_loc_name_start_list,
            toll_loc_name_end=toll_loc_name_end_list,
            toll_system_type = toll_system_type_list,
            entry_time = entry_time_list,
            exit_time = exit_time_list,
            tag_cost=tag_cost_list,
            cash_cost=cash_cost_list,
            license_plate_cost=license_plate_cost_list
        ))
        if not os.path.exists(output_dir): os.mkdir(output_dir)
        extracted_df.to_csv(output_file, index=False)
    else:
        raise FileNotFoundError(f'Cannot find the respective folder: {to_process}\nPlease try again with a valid folder with csv files.')
    
if __name__=='__main__':
    typer.run(main)