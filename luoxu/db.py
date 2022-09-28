import logging
import contextlib
from pickle import NONE
from typing import Literal, Any
import asyncio
from random import randint
import datetime
from telethon.tl.types import PeerChannel, PeerChat, User
import asyncpg

from util import format_name, UpdateDirection
from indexing import text_to_query, format_msg
from mytypes import SearchCriteria, GroupNotFound
from ctxvars import msg_source

logger = logging.getLogger('luoxu')

class PostgreStore:
  SEARCH_LIMIT = 50

  def __init__(self, config: dict[str, Any]) -> None:
    self.address = config['connection_url']
    first_year = config.get('first_year', 2016)
    self.ocr_url = config.get('ocr_url')
    self.earliest_time = datetime.datetime(first_year, 1, 1).astimezone()
    self.pool = None

  async def setup(self) -> None:
    self.pool = await asyncpg.create_pool(self.address)

  async def _insert_one_message(self, conn, msg, text, group_id):
    u = await msg.get_sender()
    reply_to_msg_id = None
    fwd_from_chat_id = None
    fwd_from_chat_post_id = None
    fwd_from_chat_name = None
    fwd_from_user_id = None
    fwd_from_user_name = None
    is_fwd = False

    if msg.reply_to_msg_id is not None:
      logger.debug('Found reply. Adding reply_to_msg_id')
      reply_to_msg_id = msg.reply_to_msg_id

    # if the message is forwarded
    if msg.forward is not None:
      is_fwd = True
      logger.debug('Found forwarded message. Processing')
      # when the forward is from a group or channel
      if msg.forward.is_channel or msg.forward.is_group:
        logger.debug('Forward is from a channel or group. Getting the group id.')
        fwd_from_chat_id = msg.forward.chat.id
        logger.debug('Forward is from a channel or group. Getting group title.')
        fwd_from_chat_name = msg.forward.chat.title
        logger.debug('Forward is from a channel or group. Getting message id if exists.')
        if msg.forward.channel_post is not None: 
          fwd_from_chat_post_id = msg.forward.channel_post

      # when the sender id is valid
      if msg.forward.sender_id is not None:
        logger.debug('Forward has valid sender_id.')
        fwd_from_user_id = msg.forward.sender_id
        logger.debug('Format sender name')
        fwd_from_user_name = format_name(msg.forward.sender)
      elif msg.forward.from_name is not None:
        logger.debug('No sender id. Use from name')
        fwd_from_user_name = msg.forward.from_name

    logger.debug('From Source "%7s" Group <%s> [%s] Chat [%s] User <%s> [%s]: %s', msg_source.get(), format_name(msg.chat), group_id, msg.id, u.id if u else 'None', format_name(u), text)

    sql = '''
      INSERT INTO messages (group_id, msg_id, from_user_id, from_user_name, msg_text, created_at, updated_at, 
      reply_to_msg_id, is_fwd, fwd_from_chat_id, fwd_from_chat_post_id, fwd_from_chat_name, fwd_from_user_id, fwd_from_user_name)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (group_id, msg_id, created_at) DO UPDATE
        SET msg_text = EXCLUDED.msg_text, updated_at = EXCLUDED.updated_at,
        reply_to_msg_id = EXCLUDED.reply_to_msg_id,
        is_fwd = EXCLUDED.is_fwd,
        fwd_from_chat_id = EXCLUDED.fwd_from_chat_id, 
        fwd_from_chat_post_id = EXCLUDED.fwd_from_chat_post_id, 
        fwd_from_chat_name = EXCLUDED.fwd_from_chat_name, 
        fwd_from_user_id  = EXCLUDED.fwd_from_user_id, 
        fwd_from_user_name  = EXCLUDED.fwd_from_user_name
    '''

    await conn.execute(sql,
      group_id,
      msg.id,
      u.id if u else None,
      format_name(u),
      text,
      msg.date,
      msg.edit_date,
      reply_to_msg_id,
      is_fwd,
      fwd_from_chat_id,
      fwd_from_chat_post_id,
      fwd_from_chat_name,
      fwd_from_user_id,
      fwd_from_user_name
    )

  async def insert_messages(self, msgs, update_direction):
    data = [(msg, text) for msg in msgs
            if (text := await format_msg(msg, self.ocr_url)) is not None]

    counter = 0
    if data:
      while True:
        try:
          async with self.get_conn() as conn:
            for msg, text in data:
        
              group_id = None
              if type(msg.peer_id) == PeerChannel:
                group_id = msg.peer_id.channel_id
              elif type(msg.peer_id) == PeerChat:
                group_id = msg.peer_id.chat_id
              else:
                group_id = msg.peer_id.user_id
        
              await self._insert_one_message(conn, msg, text, group_id)
              counter += 1
        
            break

        except asyncpg.exceptions.DeadlockDetectedError:
          t = randint(1, 50) / 10
          logger.warning('deadlock detected, retry in %.1fs', t)
          await asyncio.sleep(t)


    group_id = None
    if type(msgs[0].peer_id) == PeerChannel:
      group_id = msgs[0].peer_id.channel_id
    elif type(msgs[0].peer_id) == PeerChat:
      group_id = msgs[0].peer_id.chat_id
    else:
      group_id = msgs[0].peer_id.user_id
    try:
      async with self.get_conn() as conn:
        if update_direction in [UpdateDirection.update_forward, UpdateDirection.update_bidirectional]:
          await self.update_group_info(conn, group_id, 1, msgs[-1].id)  # update the last message id
        if update_direction in [UpdateDirection.update_backward, UpdateDirection.update_bidirectional]:
          await self.update_group_info(conn, group_id, -1, msgs[0].id)  # update the first message id
    except asyncpg.exceptions.DeadlockDetectedError:
      t = randint(1, 50) / 10
      logger.warning('deadlock detected, retry in %.1fs', t)
      await asyncio.sleep(t)

    return counter

  async def get_group_by_id(self, conn, group_id: int):
    sql = '''\
        select * from tg_groups
        where group_id = $1'''
    return await conn.fetchrow(sql, group_id)

  async def upsert_group(self, conn, group):
    g = await self.get_group_by_id(conn, group.id)
    if g:
      return g

    sql = '''\
        insert into tg_groups
        (group_id, title) values
        ($1, $2)
        returning *'''
    group_title = None
    if type(group) == User:
      group_title = format_name(group)
    else:
      group_title = group.title
    return await conn.fetchrow(
      sql,
      group.id,
      group_title
    )

  async def update_group_info(
    self, conn, group_id: int,
    direction: Literal[1, -1], msg_id: int,
  ) -> None:
    if direction == 1:
      logger.info('Update Group %s last synced message ID %s', group_id, msg_id)
      sql = '''update tg_groups set loaded_last_id = $1 where group_id = $2 and coalesce(loaded_last_id, 0) < $1'''
    elif direction == -1:
      logger.info('Update Group %s first synced message ID %s', group_id, msg_id)
      sql = '''update tg_groups set loaded_first_id = $1 where group_id = $2'''
    else:
      raise ValueError(direction)
    await conn.execute(sql, msg_id, group_id)

  @contextlib.asynccontextmanager
  async def get_conn(self):
    for i in range(5):
      try:
        async with self.pool.acquire() as conn, conn.transaction():
          yield conn
        break
      except FileNotFoundError:
        if i < 4:
          logger.error('FileNotFoundError while connecting to database, will retry later')
          await asyncio.sleep(1)
        else:
          raise

  async def search(self, q: SearchCriteria) -> list[dict]:
    async with self.get_conn() as conn:
      if q.group_id:
        group = await self.get_group_by_id(conn, q.group_id)
        if not group:
          raise GroupNotFound(q.group_id)
        groupinfo = {
          q.group_id: [group['group_id'], group['title']],
        }
      else:
        sql = '''select group_id, title from tg_groups'''
        rows = await conn.fetch(sql)
        groupinfo = {row['group_id']: row['title'] for row in rows}

    ret = []
    now = datetime.datetime.now().astimezone()
    # we search backwards, so we start in "end" year or current year
    if q.end:
      this_year = min(q.end, now).year
    else:
      this_year = now.year

    while True:
      this_year_start = datetime.datetime(this_year, 1, 1).astimezone()
      next_year_start = datetime.datetime(this_year+1, 1, 1).astimezone()
      logger.debug('this_year_start=%s, next_year_start=%s, q.end=%s', this_year_start, next_year_start, q.end)

      if q.end:
        date_end = min(q.end, next_year_start)
      else:
        date_end = next_year_start
      if q.start:
        date_start = max(q.start, this_year_start)
      else:
        date_start = this_year_start

      if date_start > date_end:
        break

      ret += await self._search_one_year(
        q, date_start, date_end,
        self.SEARCH_LIMIT - len(ret),
      )

      if len(ret) >= self.SEARCH_LIMIT or date_start < self.earliest_time:
        break

      this_year -= 1

    return groupinfo, ret

  async def _search_one_year(
    self,
    q: SearchCriteria,
    date_start: datetime.datetime,
    date_end: datetime.datetime,
    limit: int,
  ) -> list[dict]:
    async with self.get_conn() as conn:
      # run a subquery to highlight because it would highlight all
      # matched rows (ignoring limits) otherwise
      common_cols = 'msg_id, group_id, from_user_id, from_user_name, created_at, updated_at'
      sql = '''select {0}, msg_text from messages where 1 = 1'''
      highlight = None
      params = []
      if q.group_id:
        sql += f''' and group_id = ${len(params)+1}'''
        params.append(q.group_id)
      if q.terms:
        query = text_to_query(q.terms.strip())
        if not query:
          raise ValueError
        sql += f''' and msg_text &@~ ${len(params)+1}'''
        params.append(query)
        highlight = f'''pgroonga_highlight_html(msg_text, pgroonga_query_extract_keywords(${len(params)+1}), 'message_idx') as html'''
        params.append(query)
      if q.sender:
        sql += f''' and from_user_id = ${len(params)+1}'''
        params.append(q.sender)

      sql += f''' and created_at > ${len(params)+1}'''
      params.append(date_start)
      sql += f''' and created_at < ${len(params)+1}'''
      params.append(date_end)

      sql += f' order by created_at desc limit {limit}'
      if highlight:
        sql = f'select {{0}}, {highlight} from ({sql}) as t'
      sql = sql.format(common_cols)
      logger.debug('searching: %s: %s', sql, params)
      rows = await conn.fetch(sql, *params)
      return rows

  async def get_all_groups(self):
    async with self.get_conn() as conn:
      sql = '''select * from tg_groups'''
      return await conn.fetch(sql)

  async def find_names(self, group: int, q: str) -> list[tuple[str, str]]:
    q = q.strip()
    if not q:
      raise ValueError
    async with self.get_conn() as conn:
      if group:
        gq = ' and $2 = ANY (group_id)'
        args = (q, group)
      else:
        gq = ''
        args = (q,)
      sql = f'''\
        select username, user_id from usernames
        where name &@ $1{gq}
        order by last_seen desc
        limit 15;
      '''
      return [(uid, r['username'])
              for r in await conn.fetch(sql, *args)
              for uid in r['user_id']]

