import logging
import argparse
from datetime import datetime
import requests

import apache_beam as beam

# function to parse lines


def parse_lines(element):
    return element.split(",")


# Class To calculte the visit duration for each Ip adress in seconds
class CalcvisitDuration(beam.DoFn):
    def process(self, element):
        dt_format = "%Y-%m-%dT%H:%M:%S"
        start_dt = datetime.strptime(element[1], dt_format)
        end_dt = datetime.strptime(element[2], dt_format)

        diff = end_dt - start_dt

        yield [element[0], diff.total_seconds()]

# Class to get Country from Ip adress


class GetIpCountryOrigin(beam.DoFn):
    def process(self, element):
        ip = element[0]
        response = requests.get(f'http://ip-api.com/json/{ip}?fields=country')
        country = response.json()['country']

        yield [ip, country]


# function to map country to ip
# expected result will be a list of  2 elements containing The country name  and the session time
#  ex : ['United kingdom', 202.00]

def map_country_to_ip(element, ip_map):
    ip = element[0]
    return [ip_map[ip], element[1]]


# Function to run the pipeline


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input")
    parser.add_argument("--output")
    args, beam_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=beam_args) as p:
        lines = (
            p
            | "ReadFile" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            | "ParseLines" >> beam.Map(parse_lines)
        )

        duration = lines | "CalcVisitDuration" >> beam.ParDo(
            CalcvisitDuration())

        ip_map = lines | "GetIpCountryOrigin" >> beam.ParDo(
            GetIpCountryOrigin())

        result = (
            duration | "MapIpToCountry" >> beam.Map(
                map_country_to_ip, ip_map=beam.pvalue.AsDict(ip_map))
            | "AverageByCountry" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
            | "FormatOutput" >> beam.Map(lambda element: ",".join(map(str, element)))
        )

        result | "WriteOutput" >> beam.io.WriteToText(
            args.output, file_name_suffix=".csv"
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
