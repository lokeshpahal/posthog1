# Generated by Django 3.2.5 on 2022-01-20 00:15

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("posthog", "0197_plugin_is_stateless"),
    ]

    operations = [
        migrations.RemoveField(model_name="asyncmigration", name="last_error",),
        migrations.CreateModel(
            name="AsyncMigrationError",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("description", models.CharField(max_length=400)),
                (
                    "async_migration",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="posthog.asyncmigration"),
                ),
            ],
        ),
    ]
