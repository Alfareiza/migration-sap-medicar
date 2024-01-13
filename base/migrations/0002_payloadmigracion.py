# Generated by Django 4.2.2 on 2023-12-22 17:31

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('base', '0001_initial_manual'),
    ]

    operations = [
        migrations.CreateModel(
            name='PayloadMigracion',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('registrado', models.DateTimeField(auto_now_add=True)),
                ('actualizado', models.DateTimeField(auto_now=True, null=True)),
                ('enviado_a_sap', models.BooleanField(default=False)),
                ('status', models.TextField()),
                ('modulo', models.CharField(max_length=64)),
                ('ref_documento', models.CharField(max_length=32)),
                ('valor_documento', models.CharField(max_length=32)),
                ('nombre_archivo', models.CharField(max_length=128)),
                ('cantidad_lineas_documento', models.IntegerField()),
                ('payload', models.JSONField()),
                ('lineas', models.TextField()),
                ('migracion_id', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='base.registromigracion')),
            ],
            options={
                'db_table': 'sap_payloads_en_migracion',
                'unique_together': {('valor_documento', 'nombre_archivo')},
            },
        ),
    ]
