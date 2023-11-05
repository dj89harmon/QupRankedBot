import psycopg2
from datetime import datetime, timedelta
from config import getDBLogin
from vars import tierRoles, dataCenters
import pytz

db_user, new_db_password = getDBLogin()
# Connect to the PostgreSQL database
def openConn():
  conn = psycopg2.connect(
    host="#redacted",
    port="#redacted",
    user=db_user,
    password=new_db_password,
    #database="qupranked_bot_db"
    database="postgres"
  )
  return conn

def openCursor(conn):
  cursor = conn.cursor()
  return cursor

def log(user_id, username, event_type, tier_role = None, data_center_role = None):
    timestamp = datetime.now()
    conn = openConn()
    cursor = openCursor(conn)
    if event_type == "ping":
      try:
        query = '''UPDATE "QupRanked"."UserRoles" SET pinged = %s WHERE user_id = %s'''
        cursor.execute(query, (datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z'), user_id))
      except Exception as e:
         print(f"error logging the ping to the database: {e}")
    try:
      # print(f"\nwould be logging: {user_id}, {event_type}, {tier_role}, {data_center_role}, {timestamp}\n")
      query = '''INSERT INTO "QupRanked"."EventLog" (user_id, username, event_type, tier_role, data_center_role, timestamp)
                  VALUES (%s, %s, %s, %s, %s, %s)'''
      cursor.execute(query, (user_id, username, event_type, tier_role, data_center_role, timestamp))
      conn.commit()
    except Exception as e:
      print(f"Error logging event: {e}")
    finally:
      cursor.close()
      conn.close()

def updateDBByUserWithTier(user_id, username, role):
  conn = openConn()
  try:
    cursor = openCursor(conn)
    query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE user_id = {user_id}'
    cursor.execute(query)
    existing_user = cursor.fetchone()    

    if existing_user:
      column_names = [desc[0] for desc in cursor.description]
      existing_user_dict = dict(zip(column_names, existing_user))
      dc_role = existing_user_dict["data_center_role"]
      cursor.execute('UPDATE "QupRanked"."UserRoles" SET tier_role = %s, timestamp = %s WHERE user_id = %s',
                      (role, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z'), user_id))

      log(existing_user_dict["user_id"], username, "update", role, dc_role);

      conn.commit()
      if dc_role is not None:
        return dc_role
      else:
        return None


    else:
      cursor.execute('INSERT INTO "QupRanked"."UserRoles" (user_id, tier_role, timestamp) VALUES (%s, %s, %s)',
                     (user_id, role, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z')))
      conn.commit()
      log(user_id, username, "insert", role, None);
      return None

  except psycopg2.Error as e:
    print(f"Error reading user data: {e}")
  
  finally:
    cursor.close()
    conn.close()

def updateDBByUserWithDC(user_id, username, role):
  conn = openConn()
  try:
    cursor = openCursor(conn)
    query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE user_id = {user_id}'
    cursor.execute(query)
    existing_user = cursor.fetchone()

    if existing_user:
      column_names = [desc[0] for desc in cursor.description]
      existing_user_dict = dict(zip(column_names, existing_user))
      tier_role = existing_user_dict["tier_role"]
      cursor.execute('UPDATE "QupRanked"."UserRoles" SET data_center_role = %s, timestamp = %s WHERE user_id = %s',
                     (role, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z'), user_id))
      
      log(existing_user_dict["user_id"], username, "update", existing_user_dict["tier_role"], role);

      conn.commit()

      if tier_role is not None:
        return role
      else:
        return None
        
    else:
      cursor.execute('INSERT INTO "QupRanked"."UserRoles" (user_id, data_center_role, timestamp) VALUES (%s, %s, %s)',
                      (user_id, role, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z')))
      
      log(user_id, username, "insert", None, role)
      conn.commit()
      return None
      

  except psycopg2.Error as e:
    print(f"Error reading user data: {e}")
  
  finally:
    cursor.close()
    conn.close()

def deleteUser(user_id, username):
  conn = openConn()
  try:
      cursor = openCursor(conn)
      delete_query = 'DELETE FROM "QupRanked"."UserRoles" WHERE user_id = %s::numeric;'
      cursor.execute(delete_query, (user_id,))

      log(user_id, username, "delete", None, None)

  except psycopg2.Error as e:
      print(f"Error deleting user data: {e}")
  finally:
     conn.commit()
     conn.close()
  return

def deleteExpiredEntries():
  conn = openConn()
  try:
    cursor = openCursor(conn)

    timezone = pytz.timezone('America/Los_Angeles')
    threshold = datetime.now() - timedelta(minutes=60)
    #threshold = datetime.now() - timedelta(minutes=1)
    threshold = timezone.localize(threshold)
    
    delete_query = 'DELETE FROM "QupRanked"."UserRoles" WHERE timestamp < %s RETURNING *'
    cursor.execute(delete_query, (threshold,))
    
    deleted_users = cursor.fetchall()
    deleted_user_ids = []
    for user in deleted_users:
       #log(user[1], "delete", None, None)
       deleted_user_ids.append(user[1])

    conn.commit()
    return deleted_user_ids


  except psycopg2.Error as e:
   print(f"Error deleting the expired entries: {e}")
  finally:
   cursor.close()
   conn.close()

async def checkForGreenLight(dc):
  play_tiers = {
        "lf_ranked_bronze": 0,
        "lf_ranked_silver": 0, 
        "lf_ranked_gold": 0,
        "lf_ranked_platinum": 0,
        "lf_ranked_diamond": 0,
        "lf_ranked_crystal(rank)": 0
        }
  conn = openConn()
  try:
      cursor = openCursor(conn)
      query = f'SELECT tier_role FROM "QupRanked"."UserRoles" WHERE data_center_role = %s'
      cursor.execute(query, (dc,))
      rows = cursor.fetchall()

      for row in rows:
          tier_role = row[0]
          if tier_role in play_tiers:
              play_tiers[tier_role] += 1

  except psycopg2.Error as e:
      print(f"Error querying database: {e}")
  
  finally:
      cursor.close()
      conn.close()

  return play_tiers

def findLowTier(dc):
  conn = openConn()
  try:
      cursor = openCursor(conn)
      # query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE data_center_role = %s AND (tier_role = %s OR tier_role = %s OR tier_role = %s)'
      query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE data_center_role = %s AND tier_role IN (%s, %s, %s)'
      cursor.execute(query, (dc, tierRoles[0], tierRoles[1], tierRoles[2]))
      fetched_rows = cursor.fetchall()
      players = []      
      
      timezone = pytz.timezone('America/Los_Angeles')
      half_hour_ago = datetime.now() - timedelta(minutes=30)
      half_hour_ago = timezone.localize(half_hour_ago)

      for row in fetched_rows:
        pinged = row[5]
        if pinged is None or pinged < half_hour_ago:
          players.append(row[1])

      return players
  
  except psycopg2.Error as e:
      print(f"Error reading user data: {e}")

  finally:
      cursor.close()
      conn.close()

def findGoldPlat(dc):
  conn = openConn()
  try:
      cursor = openCursor(conn)
      query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE data_center_role = %s AND (tier_role = %s OR tier_role = %s)'
      cursor.execute(query, (dc, tierRoles[2], tierRoles[3]))
      fetched_rows = cursor.fetchall()
      players = []

      timezone = pytz.timezone('America/Los_Angeles')
      half_hour_ago = datetime.now() - timedelta(minutes=30)
      half_hour_ago = timezone.localize(half_hour_ago)

      for row in fetched_rows:
        pinged = row[5]
        if pinged is None or pinged < half_hour_ago:
          players.append(row[1])

      return players
  
  except psycopg2.Error as e:
      print(f"Error reading user data: {e}")

  finally:
      cursor.close()
      conn.close()

def findPlatDiamond(dc):
  conn = openConn()
  try:
      cursor = openCursor(conn)
      query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE data_center_role = %s AND (tier_role = %s OR tier_role = %s)'
      cursor.execute(query, (dc, tierRoles[3], tierRoles[4]))
      fetched_rows = cursor.fetchall()
      players = []

      timezone = pytz.timezone('America/Los_Angeles')
      half_hour_ago = datetime.now() - timedelta(minutes=30)
      half_hour_ago = timezone.localize(half_hour_ago)

      for row in fetched_rows:
        pinged = row[5]
        if pinged is None or pinged < half_hour_ago:
          players.append(row[1])

      return players
  
  except psycopg2.Error as e:
      print(f"Error reading user data: {e}")

  finally:
      cursor.close()
      conn.close()

def findDiamondCrystal(dc):
  conn = openConn()
  try:
      cursor = openCursor(conn)
      query = f'SELECT * FROM "QupRanked"."UserRoles" WHERE data_center_role = %s AND (tier_role = %s OR tier_role = %s)'
      cursor.execute(query, (dc, tierRoles[4], tierRoles[5]))
      fetched_rows = cursor.fetchall()
      players = []

      timezone = pytz.timezone('America/Los_Angeles')
      half_hour_ago = datetime.now() - timedelta(minutes=30)
      half_hour_ago = timezone.localize(half_hour_ago)

      for row in fetched_rows:
        pinged = row[5]
        if pinged is None or pinged < half_hour_ago:
          players.append(row[1])
      return players
  
  except psycopg2.Error as e:
      print(f"Error reading user data: {e}")

  finally:
      cursor.close()
      conn.close()

async def getActives():
  conn = openConn()
  cursor = openCursor(conn)

  try:
    query = 'SELECT * FROM "QupRanked"."active";'
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    result = [dict(zip(column_names, row)) for row in rows]
    return result
  except Exception as e:
     print(f"error reading actives, {e}")

async def updateActives(tierGroup, dc):
  conn = openConn()
  cursor = openCursor(conn)
  
  try:
    query = f'''UPDATE "QupRanked"."active"
                SET "{tierGroup}" = now()
                WHERE "DataCenter" = '{dc}';'''
    cursor.execute(query)
    conn.commit()

    #x = await getActives()

  except Exception as e:
     print(f"error reading actives, {e}")

def removeActives(tierGroup, dc):
  conn = openConn()
  cursor = openCursor(conn)

  try:
    query = f'''UPDATE "QupRanked"."active" SET "{tierGroup}" = NULL WHERE "DataCenter" = '{dc}' '''
    cursor.execute(query)
    conn.commit()


  except Exception as e:
     print(f"Error removing active value: {e}")

def cleanTables():
  conn = openConn()
  try:
    cursor = openCursor(conn)
    query = f'TRUNCATE TABLE "QupRanked"."UserRoles"'
    cursor.execute(query)
    conn.commit()
  except Exception as e:
    print(f"Exception: {e}")
  
  try:
      for data_center in dataCenters:
        query = f"UPDATE \"QupRanked\".\"active\" SET \"Bronze/Silver/Gold\" = NULL, \"Gold/Platinum\" = NULL, \"Platinum/Diamond\" = NULL, \"Diamond/Crystal\" = NULL WHERE \"DataCenter\" = '{data_center}'"
        cursor.execute(query)
      conn.commit() 
  except Exception as e:
    print(f"Error cleaning actives table: {e}")
  finally:
    cursor.close()
    conn.close()