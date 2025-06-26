import os
import xml.etree.ElementTree as ET
import json
import configparser
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import requests
import logging
from datetime import datetime

logging.basicConfig(filename='process_log.txt', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_config_from_ini():
    config = configparser.ConfigParser()
    ini_file = 'config.ini'

    if not os.path.exists(ini_file):
        print("config.ini bestand bestaat niet. Wordt aangemaakt met default waarden.")
        default_directory = r"C:\\Users\\nlgan\\OneDrive\\Bureaublad\\xml files"
        default_supabase_url = "https://yuqkslnbweatlbwitqls.supabase.co"
        default_supabase_api_key = "your-supabase-api-key-here"
        default_supabase_table = "public_logs"

        config['settings'] = {'directory': default_directory}
        config['supabase'] = {
            'SUPABASE_URL': default_supabase_url,
            'SUPABASE_API_KEY': default_supabase_api_key,
            'SUPABASE_TABLE': default_supabase_table
        }
        with open(ini_file, 'w') as configfile:
            config.write(configfile)
        print(f"config.ini aangemaakt met directory: {default_directory} en Supabase instellingen.")

    config.read(ini_file)
    directory = config['settings']['directory']
    supabase_url = config['supabase']['SUPABASE_URL']
    supabase_api_key = config['supabase']['SUPABASE_API_KEY']
    supabase_table = config['supabase']['SUPABASE_TABLE']
    return directory, supabase_url, supabase_api_key, supabase_table

def xml_to_json(xml_file):
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        def strip_ns(tag): 
            return tag.split('}', 1)[-1] if '}' in tag else tag

        def xml_to_dict(elem):
            result = {}
            children = list(elem)
            if children:
                temp = {}
                for child in children:
                    tag = strip_ns(child.tag)
                    val = xml_to_dict(child)[tag]
                    if tag in temp:
                        temp[tag] = temp[tag] + [val] if isinstance(temp[tag], list) else [temp[tag], val]
                    else:
                        temp[tag] = val
                result[strip_ns(elem.tag)] = temp
            else:
                result[strip_ns(elem.tag)] = elem.text.strip() if elem.text else None
            return result

        return xml_to_dict(root)

    except Exception as e:
        logging.error(f"XML conversiefout: {e}")
        return None

def save_to_supabase(supabase_url, supabase_api_key, supabase_table, request_json=None, response_json=None):
    headers = {
        "apikey": supabase_api_key,
        "Authorization": f"Bearer {supabase_api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    payload = {
        "request": request_json or {},
        "respons": response_json or {}
    }
    try:
        res = requests.post(f"{supabase_url}/rest/v1/{supabase_table}", headers=headers, json=payload)
        res.raise_for_status()
        logging.info("Data succesvol opgeslagen in Supabase.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Fout bij opslaan in Supabase: {e}")

def save_json_to_file(data, path):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Fout bij opslaan JSON-bestand: {e}")

def convert_xml_to_json(directory, xml_filename, supabase_url, supabase_api_key, supabase_table):
    xml_filepath = os.path.join(directory, xml_filename)
    json_data = xml_to_json(xml_filepath)
    if json_data is None:
        logging.error(f"Fout bij lezen/conversie: {xml_filename}")
        return

    json_path = os.path.join(directory, f"{os.path.splitext(xml_filename)[0]}.json")
    save_json_to_file(json_data, json_path)
    logging.info(f"Geconverteerd en lokaal opgeslagen: {json_path}")

    # Kijk of er een bijbehorende Reply file is
    reply_filename = f"{os.path.splitext(xml_filename)[0]}_Reply.xml"
    reply_filepath = os.path.join(directory, reply_filename)

    if os.path.exists(reply_filepath):
        response_json = xml_to_json(reply_filepath)
        if response_json is None:
            logging.error(f"Fout bij lezen/conversie Reply bestand: {reply_filename}")
            response_json = {}
    else:
        response_json = {}

    # Sla request en response samen op in Supabase
    save_to_supabase(supabase_url, supabase_api_key, supabase_table, request_json=json_data, response_json=response_json)

def process_reply_file(directory, reply_filename, supabase_url, supabase_api_key, supabase_table):
    filepath = os.path.join(directory, reply_filename)
    json_resp = xml_to_json(filepath)
    if json_resp is None:
        logging.error(f"Fout bij conversie Reply.xml: {reply_filename}")
        return

    json_path = os.path.join(directory, f"{os.path.splitext(reply_filename)[0]}.json")
    save_json_to_file(json_resp, json_path)
    logging.info(f"Reply JSON lokaal opgeslagen: {json_path}")

    # Alleen respons opslaan (request leeg)
    save_to_supabase(supabase_url, supabase_api_key, supabase_table, request_json={}, response_json=json_resp)
    logging.info(f"Reply data succesvol opgeslagen in Supabase (veld 'respons'): {reply_filename}")

class XMLFileHandler(FileSystemEventHandler):
    def __init__(self, directory, supabase_url, supabase_api_key, supabase_table):
        self.directory = directory
        self.supabase_url = supabase_url
        self.supabase_api_key = supabase_api_key
        self.supabase_table = supabase_table

    def on_created(self, event):
        if event.is_directory:
            return
        filename = os.path.basename(event.src_path)
        logging.info(f"Nieuw bestand gedetecteerd: {filename}")

        if filename.endswith("_Reply.xml"):
            process_reply_file(self.directory, filename, self.supabase_url, self.supabase_api_key, self.supabase_table)
        elif filename.endswith(".xml"):
            convert_xml_to_json(self.directory, filename, self.supabase_url, self.supabase_api_key, self.supabase_table)

def monitor_directory(directory, supabase_url, supabase_api_key, supabase_table):
    handler = XMLFileHandler(directory, supabase_url, supabase_api_key, supabase_table)
    obs = Observer()
    obs.schedule(handler, directory, recursive=False)
    obs.start()
    print(f"Monitoring directory: {directory}...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()

def main():
    directory, supabase_url, supabase_api_key, supabase_table = get_config_from_ini()
    monitor_directory(directory, supabase_url, supabase_api_key, supabase_table)

if __name__ == "__main__":
    main()
