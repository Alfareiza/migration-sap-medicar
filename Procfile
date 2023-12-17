release: python manage.py migrate --noinput;
web: gunicorn core.wsgi --log-file - --workers 1
clock: python base/management/commands/migrasap.py