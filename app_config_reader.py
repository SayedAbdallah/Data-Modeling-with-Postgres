import configparser
def get_database_configuration():
    config = configparser.ConfigParser()
    config.read('app.ini')
    return dict(config['DEFAULT'].items())