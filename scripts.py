import subprocess

from configuration import AWS_EC2_LOCATION

def scp_configuration_file():
    directory = 'mbta'
    file_name = 'aws_configuration.py'

    aws_location = AWS_EC2_LOCATION
    remote_dir = ':~/code/'
    remote_location = aws_location + remote_dir

    new_file_name = 'configuration.py'

    scp_file(remote_location, directory, file_name, new_file_name)

def scp_file(remote_location, directory, file_name, new_file_name=''):
    location = remote_location + directory + '/' + new_file_name
    args = ['scp', '-v', '-F', '~/.ssh/config', file_name, location]
    command = ' '.join([str(c) for c in args])
    print(command)
    subprocess.call(command, shell=True)