"""backfill_agent_id

Revision ID: 011e6f2dd9c2
Revises: f16d4ab03091
Create Date: 2023-09-18 11:02:36.795544

"""
from alembic import op
import datetime
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '011e6f2dd9c2'
down_revision = 'f16d4ab03091'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    chatbots_table = sa.Table(
        'chat_bots',
        sa.MetaData(),
        sa.Column('id', sa.Integer()),
        sa.Column('project_id', sa.Integer()),
        sa.Column('agent_id', sa.Integer()),
        sa.Column('name', sa.String()),
        sa.Column('model_name', sa.String())
    )

    agents_table = sa.Table(
        'agents',
        sa.MetaData(),
        sa.Column('id', sa.Integer()),
        sa.Column('company_id', sa.Integer()),
        sa.Column('user_class', sa.Integer()),
        sa.Column('name', sa.String()),
        sa.Column('project_id', sa.Integer()),
        sa.Column('model_name', sa.String()),
        sa.Column('updated_at', sa.DateTime()),
        sa.Column('created_at', sa.DateTime())
    )

    tasks_table = sa.Table(
        'tasks',
        sa.MetaData(),
        sa.Column('company_id', sa.Integer()),
        sa.Column('user_class', sa.Integer()),
        sa.Column('object_type', sa.String()),
        sa.Column('object_id', sa.Integer())
    )

    all_chatbots = conn.execute(chatbots_table.select()).fetchall()
    for chatbot_row in all_chatbots:
        id, project_id, _, name, model_name = chatbot_row

        # Get the corresponding task.
        task_select = tasks_table.select().where(tasks_table.c.object_type == 'chatbot').where(tasks_table.c.object_id == id)
        task_row = conn.execute(task_select).first()
        if task_row is None:
            continue
        company_id, user_class, _, _ = task_row
        # Create the new agent.
        op.execute(agents_table.insert().values(
            company_id=company_id,
            user_class=user_class,
            name=name,
            project_id=project_id,
            model_name=model_name,
            updated_at=datetime.datetime.now(),
            created_at=datetime.datetime.now()
        ))

        # Get the new agent and associate the chatbot with it.
        created_agent = conn.execute(agents_table.select().where(agents_table.c.name == name)).first()
        agent_id = created_agent[0]
        op.execute(chatbots_table.update().where(chatbots_table.c.id == id).values(agent_id=agent_id))


def downgrade():
    pass
