import requests
import typer
import json
import os
from dotenv import dotenv_values
from typing_extensions import Annotated



def main(
    to_process: Annotated[str, typer.Option(help="Path to the CSV folder.")],
    output_dir: Annotated[str, typer.Option(help="The folder where the resulting JSON files will be stored.")]
    ):
    """Connect to Toll Guru API and fetch trip data to JSON files.
    """
    env_vars = dotenv_values('./.env')
    url = env_vars.get('TOLLGURU_API_URL')
    
    headers = {'x-api-key': env_vars.get('TOLLGURU_API_KEY'), 'Content-Type': 'text/csv'}
    if os.path.exists(to_process):
        for file in os.listdir(to_process):
            json_file = file.replace('.csv', '.json')
            if file.endswith('.csv'):
                file_path = os.path.join(to_process, file)
                with open(file_path, 'r') as file:
                    response = requests.post(url, data=file, headers=headers)
                if response.status_code == 200:
                    # create the output directory if not exists
                    if not os.path.exists(output_dir): os.mkdir(output_dir)
                    write_path = os.path.join(output_dir, json_file)
                    with open(write_path, 'w') as json_file:
                        json.dump(response.json(), json_file, indent=4)
                else:
                    print(f"Error:\nCouldn't fetch data for file {file_path}.\nExisted with error code: {response.status_code}.\nError details: {response.text}")

                
    else:
        raise FileNotFoundError(f'Cannot find the respective folder: {to_process}\nPlease try again with a valid folder with csv files.')


if __name__=='__main__':
    typer.run(main)