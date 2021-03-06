import click
import json
import subprocess

from . import util


@click.group()
def main():
    """
    This CLI allows for configuring, installing, and deploying the gluestick API.
    To learn more, read the docs: https://docs.gluestick.xyz
    """
    pass


@main.command()
def install():
    """
    Create the gluestick.json configuration in this directory
    """
    # Create the default config
    util.create_default_config()
    click.echo(click.style('Created default gluestick-api configuration.', fg='green'))

    # Pull the Docker container
    click.echo('Pulling the gluestick-api Docker image...')
    subprocess.run("docker pull hotglue/gluestick-api", shell=True, check=True)
    click.echo(click.style('Latest gluestick-api Docker image pulled.', fg='green'))


@main.command()
@click.option("--port", default=5000, type=int)
def run(port):
    """
    Run the gluestick-api on the local machine
    """
    # Verify that gluestick config exists
    util.config_exists()

    click.echo(click.style('Starting gluestick-api...', fg='green'))

    # Start the Docker container
    subprocess.run(f"docker run --env-file gluestick.config -e PORT={port} -p {port}:{port} -it hotglue/gluestick-api", shell=True, check=True)


@main.group()
def config():
    """
    Configure the gluestick-api
    """
    # Verify that gluestick config exists
    util.config_exists()
    pass


def target_args(ctx, param, target_name):
    # TODO: Add support for google cloud storage

    if target_name == "s3":
        # Handle prompts for S3 data
        bucket = click.prompt("What S3 bucket do you want to use?")
        path_prefix = click.prompt("What path prefix do you want files to be uploaded under?", default="uploads/{user}")
        aws_access_key_id = click.prompt("What is your AWS Access Key Id?")
        aws_secret_access_key = click.prompt("What is your AWS Secret Access Key?", hide_input=True, confirmation_prompt=True)

        return (target_name, bucket, path_prefix, aws_access_key_id, aws_secret_access_key)


@config.command()
@click.option("--format", prompt="What format do you want final data in?", type=click.Choice(['json', 'csv'], case_sensitive=False))
@click.option("--dest", prompt="Where do you want final data to go?", type=click.Choice(['s3'], case_sensitive=False), callback=target_args)
def target(format, dest):
    """
    Configure the target destination for final data
    """
    target_name = dest[0]
    click.echo("Using target {}".format(target_name))

    if target_name == "s3":
        target_config = {
            'GLUESTICK_TARGET': 's3',
            'GLUESTICK_TARGET_FORMAT': format,
            'GLUESTICK_TARGET_BUCKET': dest[1],
            'GLUESTICK_TARGET_PATH_PREFIX': dest[2],
            'GLUESTICK_TARGET_AWS_ACCESS_KEY_ID': dest[3],
            'GLUESTICK_TARGET_AWS_SECRET_ACCESS_KEY': dest[4]
        }

        # Update the gluestick config
        util.update_config('target', target_config)
        click.echo(click.style('Configuration saved', fg='green'))


@config.command()
@click.option("--url", prompt="What endpoint is your listener on?")
@click.option("--secret")
def webhook(url, secret):
    """
    Configure a webhook endpoint to receive updates on user imports
    """
    config = {
        'GLUESTICK_WEBHOOK_URL': url
    }

    if secret is not None:
        config['GLUESTICK_WEBHOOK_SECRET']= secret

    # Update the gluestick config
    util.update_config('webhook', config)
    click.echo(click.style('Configuration saved', fg='green'))
