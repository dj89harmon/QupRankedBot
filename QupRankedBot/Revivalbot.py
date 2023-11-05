import discord
import asyncio
from datetime import datetime, timedelta
import dbfunctions
from discord.utils import get
from discord.ext import commands
from config import getToken, guildId, channelId, tierMessageID, DCMessageID, updateMessageID
from vars import tiers, dataCenters, tierRoles, dataCenterRoles
import pytz
import time

BOT_TOKEN = getToken()

intents = discord.Intents.default()
intents.typing = False
intents.presences = False
intents.members = True
intents.messages = True
intents.message_content = True

rankedCommand = datetime.now()

guild = None
channel = None
tierMessage = None
DCMessage = None
updateMessage = None

bot = commands.Bot(command_prefix="!", intents=intents)

db_lock = None
event_queue = None

@bot.event
async def on_ready():
    global db_lock, event_queue
    db_lock = asyncio.Lock()
    event_queue = asyncio.Queue()
    
    global guild, channel, tierMessage, DCMessage, updateMessage
    print(f"Bot connected as {bot.user}")

    guild = bot.get_guild(guildId)
    channel = guild.get_channel(channelId)
    tierMessage = await channel.fetch_message(tierMessageID)
    DCMessage = await channel.fetch_message(DCMessageID)
    updateMessage = await channel.fetch_message(updateMessageID)

    dbfunctions.cleanTables()
    await updateActives()
    await process_event_queue()

async def handleTierRole(payload):
    global guild, channel, tierMessage
    def get_role_name_from_emoji(emoji_name):
        emoji_to_role = {
            'U+1F1E7': tierRoles[0],
            'U+1F1F8': tierRoles[1],
            'U+1F1EC': tierRoles[2],
            'U+1F1F5': tierRoles[3],
            'U+1F1E9': tierRoles[4],
            'U+1F1E8': tierRoles[5]
        }
        return emoji_to_role.get(emoji_name, None)

    user_id = payload.user_id
    emoji = payload.emoji
    emoji_name = ' '.join([f'U+{ord(c):X}' for c in emoji.name])
    
    message = tierMessage
    user = await guild.fetch_member(user_id)
    await message.remove_reaction(payload.emoji, user)
    roles_to_remove = tierRoles
    
    for role_name in roles_to_remove:
        role = discord.utils.get(guild.roles, name=role_name)
        if role is not None and role in user.roles:
            await user.remove_roles(role)

    role_name = get_role_name_from_emoji(emoji_name)
    if role_name is not None:
        await addRole(guild, role_name, user)
        ping = dbfunctions.updateDBByUserWithTier(user_id, user.name, role_name)
    return ping, role_name, user

    
async def handleDCRole(payload):
    global guild, channel, DCMessage
    def get_role_name_from_emoji(emoji_name):
        emoji_to_role = {
            'U+1F1E6': dataCenterRoles[0],
            'U+1F1E8': dataCenterRoles[1],
            'U+1F1E9': dataCenterRoles[2],
            'U+1F1F5': dataCenterRoles[3],
            'U+1F1ED': dataCenterRoles[4],
            'U+1F1F1': dataCenterRoles[5]
        }
        return emoji_to_role.get(emoji_name, None)

    user_id = payload.user_id
    emoji = payload.emoji
    emoji_name = ' '.join([f'U+{ord(c):X}' for c in emoji.name])

    message = DCMessage
    user = await guild.fetch_member(user_id)
    await message.remove_reaction(payload.emoji, user)
    roles_to_remove = dataCenterRoles

    for role_name in roles_to_remove:
        role = discord.utils.get(guild.roles, name=role_name)
        if role is not None and role in user.roles:
            await user.remove_roles(role)

    role_name = get_role_name_from_emoji(emoji_name)
    if role_name is not None:
        await addRole(guild, role_name, user)
        ping = dbfunctions.updateDBByUserWithDC(user_id, user.name, role_name)
    return ping, role_name, user

async def handleResetOptions(payload):
    global guild, channel, updateMessage
    message = await channel.fetch_message(payload.message_id)
    #message = updateMessage
    user = await guild.fetch_member(payload.user_id)
    await message.remove_reaction(payload.emoji, user)
    emoji = payload.emoji
    emojiName = ' '.join([f'U+{ord(c):X}' for c in emoji.name])

    if emojiName == 'U+274C':
      roles_to_remove = tierRoles + dataCenterRoles
      for role_name in roles_to_remove:
          role = discord.utils.get(user.roles, name=role_name)
          if role is not None:
              await user.remove_roles(role)
      dbfunctions.deleteUser(payload.user_id, user.name)
      try:
          await user.send("lf_ranked roles deleted")
      except Exception as e:
          print(f"error messaging user: {e}")
    elif emojiName == 'U+1F9F9':
      async for message in user.history():
         if message.author == bot.user and isinstance(message.channel, discord.DMChannel):
            await message.delete()    

      return
    return

async def checkForGreenLight(dc):
  currTiers = await dbfunctions.checkForGreenLight(dc)
  pinged = set([])
  update = False

  print(f"\ntier totals for {dc}:\nlow tier total: {(currTiers[tierRoles[0]] + currTiers[tierRoles[1]] + currTiers[tierRoles[2]])}")
  print(f"gold/plat total: {(currTiers[tierRoles[2]] + currTiers[tierRoles[3]])}")
  print(f"plat/dia tier total: {(currTiers[tierRoles[3]] + currTiers[tierRoles[4]])}")
  print(f"dia/crystal tier total: {(currTiers[tierRoles[4]] + currTiers[tierRoles[5]])}\n")
  if (currTiers[tierRoles[0]] + currTiers[tierRoles[1]] + currTiers[tierRoles[2]]) >= 10:
    print(f"pinging low tier: {dc}")
    pinged.update(set(await pingLowTier(dc)))
    update = True
  if (currTiers[tierRoles[2]] + currTiers[tierRoles[3]]) >= 10:
    print("pinging gold/plat")
    pinged.update(set(await pingGoldPlat(dc, pinged)))
    update = True
  if (currTiers[tierRoles[3]] + currTiers[tierRoles[4]]) >= 10:
    print("pinging plat/dia")
    pinged.update(set(await pingPlatDiamond(dc, pinged)))
    update = True
  if (currTiers[tierRoles[4]] + currTiers[tierRoles[5]]) >= 10:
    print("pinging dia/crystal")
    pinged.update(set(await pingDiamondCrystal(dc, pinged)))
    update = True
  if update:
     await updateActives()
  

async def pingLowTier(dc):
    players = dbfunctions.findLowTier(dc)
    for player in players:
      try:
          user = await bot.fetch_user(player)
          username = user.name
          try:
              await user.send("You've got a game ready to pop, qup ranked!")
          except discord.Forbidden:
              print(f"Bot does not have permission to send messages to user with id {player}")
          await dbfunctions.updateActives('Bronze/Silver/Gold', dataCenters[dataCenterRoles.index(dc)])
          dbfunctions.log(player, username,  "ping", "lowtier", dc)
      except discord.NotFound:
          print(f"User with id {player} not found")
      except discord.Forbidden:
          print(f"Bot does not have permission to send messages to user with id {player}")
    #await updateActives()
    return players

async def pingGoldPlat(dc, pinged):
    players = dbfunctions.findGoldPlat(dc)
    for player in players:
      if player in pinged:
          print(f"player {player} has already been pinged")
          continue
      try:
          user = await bot.fetch_user(player)
          username = user.name
          try:
              await user.send("You've got a game ready to pop, qup ranked!")
          except discord.Forbidden:
              print(f"Bot does not have permission to send messages to user with id {player}")
          await dbfunctions.updateActives('Gold/Platinum', dataCenters[dataCenterRoles.index(dc)])
          dbfunctions.log(player, username, "ping", "gold/plat", dc)
      except discord.NotFound:
          print(f"User with id {player} not found")
      except discord.Forbidden:
          print(f"Bot does not have permission to send messages to user with id {player}")
    #await updateActives()
    return players

async def pingPlatDiamond(dc, pinged):
    players = dbfunctions.findPlatDiamond(dc)
    for player in players:
      if player in pinged:
          continue
      try:
          user = await bot.fetch_user(player)
          username = user.name
          try:
              await user.send("You've got a game ready to pop, qup ranked!")
          except discord.Forbidden:
              print(f"Bot does not have permission to send messages to user with id {player}")
          await dbfunctions.updateActives('Platinum/Diamond', dataCenters[dataCenterRoles.index(dc)])
          dbfunctions.log(player, username, "ping", "plat/diamond", dc)
      except discord.NotFound:
          print(f"User with id {player} not found")
      except discord.Forbidden:
          print(f"Bot does not have permission to send messages to user with id {player}")
    #await updateActives()
    return players

async def pingDiamondCrystal(dc, pinged):
    players = dbfunctions.findDiamondCrystal(dc)

    for player in players:
      if player in pinged:
          continue
      try:
          user = await bot.fetch_user(player)
          username = user.name
          try:
              await user.send("You've got a game ready to pop, qup ranked!")
          except discord.Forbidden:
              print(f"Bot does not have permission to send messages to user with id {player}")
          await dbfunctions.updateActives('Diamond/Crystal', dataCenters[dataCenterRoles.index(dc)])
          dbfunctions.log(player, username, "ping", "diamond/crystal", dc)
      except discord.NotFound:
          print(f"User with id {player} not found")
      
    #await updateActives()
    return players

@bot.command()
@commands.is_owner()
async def writeTest(ctx):
   await updateActives()

@bot.event
async def on_raw_reaction_add(payload):
    if payload.message_id == #redacted:
      await handleResetOptions(payload)
      return
    
    await event_queue.put(payload)
    if not db_lock.locked():
        await process_event_queue()
@bot.command()
async def qdown(ctx, overload = None):
  try:  
    if overload is not None:
        message = await ctx.channel.fetch_message(ctx.message.id)
        await message.add_reaction("\u274C")
        return
    roles_to_remove = tierRoles + dataCenterRoles
    user = ctx.author
    user_id = ctx.author.id
    user_name = ctx.author.name
    
    for role_name in roles_to_remove:
        role = discord.utils.get(user.roles, name=role_name)
        if role is not None:
            await user.remove_roles(role)
    dbfunctions.deleteUser(user_id, user_name)
    try:

        await user.send("lf_ranked roles deleted")
        message = await ctx.channel.fetch_message(ctx.message.id)
        await message.add_reaction("\u2705")
    except Exception as e:
        print(f"error messaging user: {e}")
  except Exception as e:
      print(f"Error in qdown: {e}")
      message = await ctx.channel.fetch_message(ctx.message.id)
      await message.add_reaction("\u274C")
    
@bot.command()
async def qup(ctx, tier = None, dataCenter = None, overload = None):
    if tier is None and dataCenter is None:
        message = await ctx.channel.fetch_message(ctx.message.id)
        await message.add_reaction("\u2705")
        return
    if dataCenter is None or overload is not None:
        message = await ctx.channel.fetch_message(ctx.message.id)
        await message.add_reaction("\u274C")
        return
        
    tier = tier.lower()
    dataCenter = dataCenter.lower().capitalize()
    
    if tier not in tiers or dataCenter not in dataCenters or tier is None or dataCenter is None:
        print(f"{tier} or {dataCenter} does not exist")
        message = await ctx.channel.fetch_message(ctx.message.id)
        await message.add_reaction("\u274C")
        return

    async with db_lock:
        global guild, channel

        user_id = ctx.author.id       
        user = await guild.fetch_member(user_id)
        
        roles_to_remove = tierRoles
        
        for tier_role_name in roles_to_remove:
            role = discord.utils.get(guild.roles, name=tier_role_name)
            if role is not None and role in user.roles:
                await user.remove_roles(role)

        tier_role_name = tierRoles[tiers.index(tier)]
        await addRole(guild, tier_role_name, user)
        ping = dbfunctions.updateDBByUserWithTier(user_id, user.name, tier_role_name)
        

        if user is not None:
            await messageMemberWithAddedRole(guild.id, user_id, tier_role_name)
        
        roles_to_remove = dataCenterRoles
        for dc_role_name in roles_to_remove:
            role = discord.utils.get(guild.roles, name=dc_role_name)
            if role is not None and role in user.roles:
                await user.remove_roles(role)
        dc_role_name = dataCenterRoles[dataCenters.index(dataCenter)]
        await addRole(guild, dc_role_name, user)
        ping = dbfunctions.updateDBByUserWithDC(user_id, user.name, dc_role_name)
        if user is not None:
            await messageMemberWithAddedRole(guild.id, user_id, dc_role_name)
        try:
             message = await ctx.channel.fetch_message(ctx.message.id)
             #emoji = '\N{white_check_mark}'
             await message.add_reaction("\u2705")
             #print(":white_check_mark: reaction added")
        except Exception as e:
             print(f"Error reacting to message: {e}")
        await checkForGreenLight(ping)
        return ping, role_name, user        
    
    
async def process_event_queue():
    queue_start_time = datetime.now()
    ping = None
    while not event_queue.empty():
        payload = await event_queue.get()
        try:
            user = await guild.fetch_member(payload.user_id)
            if payload.message_id == tierMessageID:
                async with db_lock:
                  ping, role_name, user = await handleTierRole(payload)
                if user is not None:
                    await tierMessage.remove_reaction(payload.emoji, user)
                    await messageMemberWithAddedRole(payload.guild_id, payload.user_id, role_name)
            elif payload.message_id == DCMessageID:
                async with db_lock:
                  start_time = datetime.now()
                  ping, role_name, user = await handleDCRole(payload)
                if user is not None:
                    await DCMessage.remove_reaction(payload.emoji, user)
                    await messageMemberWithAddedRole(payload.guild_id, payload.user_id, role_name)

        except Exception as e:
            print(f"Error handling event: {e}")
        finally:
            event_queue.task_done()
    
    end_time = datetime.now()
    elapsed_time = end_time - queue_start_time
    
    if ping is not None:

        await checkForGreenLight(ping)    

async def addRole(guild, role_name, user):
    role = discord.utils.get(guild.roles, name=role_name)
    await user.add_roles(role)    
    return
        
async def messageMemberWithAddedRole(guild_id, user_id, role):
    global guild
    member = await guild.fetch_member(user_id)
    if member is not None:
      try:
        await member.send(f"Role added: {role}")
      except Exception as e:
        print(f"Error messaging member with their role: {e}")
    else:
      return

@bot.command()
async def delete_dms(ctx):
    async for message in ctx.author.history():
        if message.author == bot.user and isinstance(message.channel, discord.DMChannel):
            await message.delete()
    #await ctx.send(f"{datetime.now()} - DM messages deleted.")

@bot.command()
@commands.is_owner()
async def rm(ctx, message_id, emoji_name): #react_to_message
    print(f"emoji_name: {emoji_name}")
    message = await ctx.channel.fetch_message(message_id)
    await message.add_reaction(emoji_name)

async def updateActives():
  try:
      global updateMessage
      result = await dbfunctions.getActives()
      updated_lines = []

      for dc in dataCenters:
        updated_line = f'{dc}:'
        for obj in result:
          if obj['DataCenter'] == dc:
              for key, value in obj.items():
                  if key != 'DataCenter' and value is not None:
                      updated_line += f'  {key}'
        updated_lines.append(updated_line)
        
      output = '\n**__Currently popping__**:\n'
      for line in updated_lines:
         output += line + "\n"
      
      embed = discord.Embed(
                title=output,
                description='',
                color=0x87CEEB
              )
     
      await updateMessage.edit(embed=embed)
      return output
  except Exception as e:
      print(f"Error updatingActives(): {e}")

@bot.command()
async def ranked(ctx):
    global rankedCommand
    try:
        current_datetime = datetime.now()
        time_difference = current_datetime - rankedCommand
        if(time_difference.total_seconds() < 3):
            print("tried to do !ranked in less than 3 seconds. Returning.")
            return
        else:
            rankedCommand = datetime.now()
        disclaimer = "**Only shows what has popped with the bot, other tiers may be active too!**\n"
        actives = await updateActives()
        #lines = actives.splitlines()
        #actives = '\n'.join(lines[2:])
        output = actives + "\n" + disclaimer
        channel = ctx.channel
        await channel.send(output)
    except Exception as e:
        print(f"Error printing !ranked: {e}")

@bot.command()
@commands.is_owner()  # Only allow the bot owner to use this command
async def stop(ctx):
  await ctx.send("Stopping the bot...")
  loop = asyncio.get_event_loop()
  for task in asyncio.all_tasks(loop=loop):
     task.cancel()
  loop.stop()
  await ctx.send("Bot has been stopped")

async def deleteExpiredEntriesTask():
    while True:
        deleted_ids = dbfunctions.deleteExpiredEntries()
        for deleted_id in deleted_ids:
          roles_to_remove = tierRoles + dataCenterRoles
          guild = bot.get_guild(guildId)
          if guild is None:
             continue
          try:
              user = await guild.fetch_member(deleted_id)
              for role_name in roles_to_remove:
                  role = discord.utils.get(user.roles, name=role_name)
                  if role is not None:
                      await user.remove_roles(role)                 
                      username = user.name
                      dbfunctions.log(deleted_id, username, "timeout", None, None)
              try:
                  await user.send("An hour has gone by since you added your lf_ranked roles, so they have been removed.  Please refresh them if you'd like to continue waiting for ranked games.")
              except Exception as e:
                  print(f"Error messaging user: {e}")
          except Exception as e:
              print(f"Error finding user: {e}")
        await asyncio.sleep(30)
  
async def resetInactiveNodesTask():
    while True:
        result = await dbfunctions.getActives()  # Retrieve the current data from the "active" table

        # Iterate through each node and check if it's older than half an hour
        for obj in result:
            for key, value in obj.items():
                if key == 'DataCenter':
                   dc = value
                if key != 'DataCenter' and value is not None:
                    delta = timedelta(minutes=45)
                    #delta = timedelta(minutes=1)
                    now = datetime.now(pytz.timezone("America/Los_Angeles"))
                    if now - value > delta: #30
                        dbfunctions.removeActives(key, dc)
                        await updateActives()
        await asyncio.sleep(60)  # Wait for 1 minute, sleep(600) would be 10 minutes
  
async def main():
    bot_task = asyncio.create_task(bot.start(BOT_TOKEN))
    delete_task = asyncio.create_task(deleteExpiredEntriesTask())
    reset_task = asyncio.create_task(resetInactiveNodesTask())

    try:
        await asyncio.gather(bot_task, delete_task, reset_task)
    except KeyboardInterrupt:
        print("Program interrupted. Closing gracefully...")
        # Perform any necessary cleanup or closing operations here

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Program interrupted manually. Exiting...")