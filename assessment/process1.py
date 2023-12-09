#!/usr/bin/env python3

import pandas as pd
import os
import typer
from typing_extensions import Annotated


def csvs_by_trip(row, outputdir):
    """Extract csv files based on the trip number."""
    
    row['time_diff'] = (row['timestamp'] - row['timestamp'].shift(1)).fillna(pd.Timedelta(seconds=0))
    row['trip_no'] = (row['time_diff'] > pd.Timedelta(hours=7)).cumsum() + 0
    for trip_no, trip_data in row.groupby('trip_no'):
        if not os.path.exists(outputdir): os.mkdir(outputdir)
        
        file_name = f"{outputdir}/{trip_data['unit'].iloc[0]}_{trip_no}.csv"
        trip_data['timestamp'] = trip_data['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        trip_data[['latitude', 'longitude', 'timestamp']].to_csv(file_name, index=False)


def main(
    to_process: Annotated[str, typer.Option(help="Path to the Parquet file to be processed.")],
    output_dir: Annotated[str, typer.Option(help="The folder to store the resulting CSV files.")]
    ):
    """Reads a parquet file and extract csv files based on different trips."""
    
    if to_process.endswith('.parquet'): # if it's a parquet file read it.
        try:
            df= pd.read_parquet(to_process)
            df['timestamp'] = pd.to_datetime(df['timestamp'],format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
            df = df.sort_values(by=['unit','timestamp'])
            df.groupby('unit').apply(csvs_by_trip, output_dir)
        except Exception as e:
            raise MemoryError(e)
    else:
        raise TypeError('Input file is not of type parquet.\nPlease try again with a parquet file.')


if __name__ == "__main__":
    typer.run(main)