#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Set bundle_name non-nullable for legacy DAGs upgraded from 2.x.

Revision ID: 35ab6b577738
Revises: 1d6611b6ab7c
Create Date: 2026-03-05 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.sql import text

from airflow.migrations.db_types import StringID

# revision identifiers, used by Alembic.
revision = "35ab6b577738"
down_revision = "1d6611b6ab7c"
branch_labels = None
depends_on = None

airflow_version = "3.2.1"


def upgrade():
    """Set bundle_name to 'dags-folder' for legacy DAGs with NULL bundle_name and make column non-nullable."""
    dialect_name = op.get_bind().dialect.name

    if dialect_name == "sqlite":
        op.execute(text("PRAGMA foreign_keys=OFF"))

    # Set any remaining NULL bundle_name values (e.g. DAGs from Airflow 2.x that skipped 3.1.x)
    op.execute(text("UPDATE dag SET bundle_name = 'dags-folder' WHERE bundle_name IS NULL"))

    with op.batch_alter_table("dag", schema=None) as batch_op:
        # Drop the foreign key before altering the column — required for MySQL to avoid error 3780
        # ("Referencing column and referenced column in foreign key constraint are incompatible")
        batch_op.drop_constraint(batch_op.f("dag_bundle_name_fkey"), type_="foreignkey")
        batch_op.alter_column("bundle_name", nullable=False, existing_type=StringID())

    # Recreate the foreign key after the column has been altered
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.create_foreign_key(
            batch_op.f("dag_bundle_name_fkey"), "dag_bundle", ["bundle_name"], ["name"]
        )

    if dialect_name == "sqlite":
        op.execute(text("PRAGMA foreign_keys=ON"))


def downgrade():
    """Revert bundle_name column to nullable."""
    dialect_name = op.get_bind().dialect.name

    if dialect_name == "sqlite":
        op.execute(text("PRAGMA foreign_keys=OFF"))

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("dag_bundle_name_fkey"), type_="foreignkey")
        batch_op.alter_column("bundle_name", nullable=True, existing_type=StringID())

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.create_foreign_key(
            batch_op.f("dag_bundle_name_fkey"), "dag_bundle", ["bundle_name"], ["name"]
        )

    if dialect_name == "sqlite":
        op.execute(text("PRAGMA foreign_keys=ON"))
