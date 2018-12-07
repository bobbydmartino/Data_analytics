import json
import numpy as np
import pandas as pd
import cPickle as pickle


class Zen(object):
    def __init__(self,jfile):
        
        self.b_labels = ['pid','name','year','team','GP','GS','AB','H','2b','3b','HR','R','RBI','SB','BB',
                  'K','E','AVG','OBP','SLG','OPS','Age','tid','pos']
        self.p_labels = ['pid','name','year','team','GP','GS','IP','W','L','S','ER','K','BB','HRA','ERA','K9',
                    'BB9','HR9','Age','tid','pos']

        self.career_b = ['pid','name','team','hof','GP','GS','AB','H','2b','3b','HR','R','RBI',
                    'SB','BB','K','E','AVG','OBP','SLG','OPS']
        self.career_p = ['pid','name','team','hof','GP','GS','IP','W','L','S','ER','K',
                    'BB','HRA','ERA','K9','BB9','HR9']     

        
        if jfile != None:
            self.load_data(jfile)
        else:
            self.tids,self.years,self.teams,self.player_list = self.load_preprocessed()
            
        self.db_df=self.get_db_df()
        self.generate_teams_stats()
        

        return
    
    
    def load_data(self,jfile):
        f = open(jfile, 'r')
        data = json.load(f)
        f.close()
        self.teams = ['ATL','BAL','BOS','CHI','CIN','CLE','DAL','DEN','DET','HOU','LV','LA','MXC','MIA','MIN','MON','NYC',
                 'PHI','PHO','PIT','POR','SAC','SD','SF','SEA','STL','TPA','TOR','VAN','WAS','Retired','Prospect','FA']
        tids = [w for q in [zip(z,self.teams) for z in [[x for y in range(len(self.teams))] for x in range(2017,1+int(data['meta']['phaseText'][:4]))]] for w in q] #long ass comprehension
        self.tids = {tids[x]:x for x in range(len(tids))}

        self.years = range(2017,int(data['meta']['phaseText'][:4]))

        self.player_list = []
        for idnum in range(len(data['players'])):
            self.player_list.append(self.generate_player_dict(idnum,data))
        return
    
    def load_preprocessed(self):
        with open('file.pickle', 'rb') as filen:
             read_dict = pickle.load(filen)
        return read_dict['tids'],read_dict['years'],read_dict['teams'],read_dict['player_list']
    
    def write_preprocessed(self):
        write_dict = {"tids":self.tids,"player_list":self.player_list,"teams":self.teams,"years":self.years}
        with open('file.pickle', 'w') as file:
            file.write(pickle.dumps(write_dict)) 
        return
    
    def get_db_df(self):
        dbs = {}
        dbs['career_reg_pitching'] = (self.career_p,False)
        dbs['career_reg_batting' ] = (self.career_b,False)
        dbs['career_post_batting' ] = (self.career_b,False)
        dbs['career_post_pitching'] = (self.career_p,False)
        dbs['post_pitching'] = (self.p_labels,True)
        dbs['reg_pitching'] = (self.p_labels,True)
        dbs['post_batting'] = (self.b_labels,True)
        dbs['reg_batting'] = (self.b_labels,True)

        db_df = {}
        for x in dbs.keys():
            val = dbs[x]
            db_df[x] = self.create_db(x,val[0],single=val[1])
        return db_df
    
    
    def create_db(self,stat,lab,single=True):
        pl = self.player_list
        if single:
            single_season = []
            for x in pl:
                if x['stats']:
                    single_season += [y for y in x[stat] if dict(zip(lab,y))['GP'] != 0]
            df = pd.DataFrame(single_season,columns=lab)
            return df
        else:
            career = []
            for x in pl:
                if x['stats']:
                    if stat in x.keys():
                        career.append(x[stat])
            df = pd.DataFrame(career,columns=lab)
            return df
        return None

    def get_league_leaders(self,df_str,value,ascending=False,head=1):
        df = self.db_df[df_str]
        ll = {}
        for x in sorted(list(df['year'].value_counts().index)):
            ll[x] = df[df['year']==x].sort_values(value,ascending=ascending).head(head)[['name','year','team',value]]
        return ll

    def get_team_pitching_df(self,df_str):
        teams = []
        df = self.db_df[df_str] 

        for x in self.tids.keys():
            tid = self.tids[x]
            
            df_real = df[df['tid']==tid]

            if x[1] in ['Retired','Prospect','FA']:
                continue

            temp = df_real[['IP','W','L','S','ER','K','BB','HRA']].sum(axis=0)

            vals = list(temp.values)
            #era
            vals.append(float(temp['ER'])/float(max(temp['IP'],1))*9)
            #k9
            vals.append(float(temp['K'])/float(max(temp['IP'],1))*9)
            #bb9
            vals.append(float(temp['BB'])/float(max(temp['IP'],1))*9)
            #k9
            vals.append(float(temp['HRA'])/float(max(temp['IP'],1))*9)

            vals = [tid,x[0],x[1]] + vals
            teams.append(vals)

        teams_df = pd.DataFrame(teams,columns=['tid','year','team','IP','W','L','S','ER','K','BB',
                                               'HRA','ERA','K9','BB9','HR9']).sort_values(['year','team'])
        return teams_df



    def get_team_batting_df(self,df_str):
        teams = []
        df = self.db_df[df_str] 

        for x in self.tids.keys():
            tid = self.tids[x]
            
            df_real = df[df['tid']==tid]

            if x[1] in ['Retired','Prospect','FA']:
                continue

            temp = df_real[['AB','H','2b','3b','HR','RBI','R','SB','BB','K','E']].sum(axis=0)

            vals = list(temp.values)
            #ba
            vals.append(float(temp['H'])/float(max(temp['AB'],1)))
            #obp
            vals.append(float(temp['H']+temp['BB'])/float(max(temp['AB'],1)))
            #slg
            vals.append(float(temp['H']+temp['2b']+2*temp['3b']+3*temp['HR'])/float(max(temp['AB'],1)))
            #ops
            vals.append(float(vals[-2])+float(vals[-1]))

            vals = [tid,x[0],x[1]] + vals
            teams.append(vals)

        teams_df = pd.DataFrame(teams,columns=['tid','year','team','AB','H','2b','3b','HR','RBI','R','SB','BB','K',
                                               'E','AVG','OBP','SLG','OPS']).sort_values(['year','team'])
        return teams_df


    def generate_awards(self,awards):
        aw = []
        for x in awards:
            aw.append(x['type'])

        d = {'roy':0,'mvp':0,'ss':0,'ws':0,'wsmvp':0,'cy':0}
        for x in aw:
            if x == u'Rookie of the Year':
                d['roy'] = 1
            if x[:14] == u'Silver Slugger':
                d['ss'] += 1
            if x == u'Most Valuable Player':
                d['mvp'] += 1
            if x == u'World Series MVP':
                d['wsmvp'] += 1
            if x == u'Won World Series':
                d['ws'] += 1
            if x == u'Cy Young Award':
                d['cy'] += 1
        return d

    
    def generate_teams_stats(self):
        # post/reg batting/pitching
        self.teams_dict = {}

        for df_str in ['reg_pitching','post_pitching']:
            self.teams_dict[df_str] = self.get_team_pitching_df(df_str)
        for df_str in ['reg_batting','post_batting']:
            self.teams_dict[df_str] = self.get_team_batting_df(df_str)

        return
    
    def generate_career_stats(self,player):
        reg = player['reg_batting']
        post = player['post_batting']

        df = pd.DataFrame(reg,columns=self.b_labels)
        df.drop(['pid','name','year','team','Age','AVG','OBP','SLG','OPS','tid','pos'], axis=1, inplace=True)
        temp = df.sum(axis=0)
        vals = list(df.sum(axis=0).values)
        #ba
        vals.append(float(temp['H'])/float(max(temp['AB'],1)))
        #obp
        vals.append(float(temp['H']+temp['BB'])/float(max(temp['AB'],1)))
        #slg
        vals.append(float(temp['H']+temp['2b']+2*temp['3b']+3*temp['HR'])/float(max(temp['AB'],1)))
        #ops
        vals.append(float(vals[-2])+float(vals[-1]))
        vals = [player['pid'],player['name'],player['team'],player['hof']] + vals
        player['career_reg_batting'] = vals

        if len(post) > 0:
            df = pd.DataFrame(post,columns=self.b_labels)
            df.drop(['pid','name','year','team','Age','AVG','OBP','SLG','OPS','tid','pos'], axis=1, inplace=True)
            temp = df.sum(axis=0)
            vals = list(df.sum(axis=0).values)
            #ba
            vals.append(float(temp['H'])/float(max(temp['AB'],1)))
            #obp
            vals.append(float(temp['H']+temp['BB'])/float(max(temp['AB'],1)))
            #slg
            vals.append(float(temp['H']+temp['2b']+2*temp['3b']+3*temp['HR'])/float(max(temp['AB'],1)))
            #ops
            vals.append(float(vals[-2])+float(vals[-1]))
            vals = [player['pid'],player['name'],player['team'],player['hof']] + vals
            player['career_post_batting'] = vals

        reg = player['reg_pitching']
        post = player['post_pitching']

        df = pd.DataFrame(reg,columns=self.p_labels) #add in self.b_labels
        df.drop(['pid','name','year','team','Age','ERA','K9','BB9','HR9','tid','pos'], axis=1, inplace=True) #drop b_labels
        temp = df.sum(axis=0)
        vals = list(df.sum(axis=0).values)
        #era
        vals.append(float(temp['ER'])/float(max(temp['IP'],1))*9)
        #k9
        vals.append(float(temp['K'])/float(max(temp['IP'],1))*9)
        #bb9
        vals.append(float(temp['BB'])/float(max(temp['IP'],1))*9)
        #k9
        vals.append(float(temp['HRA'])/float(max(temp['IP'],1))*9)
        vals = [player['pid'],player['name'],player['team'],player['hof']] + vals
        intvals = ['W','L','S','ER','K','BB','HRA']
        d = dict(zip(self.career_p,vals))
        for i in intvals:
            d[i] = int(d[i])
        vals = [d[k] for k in self.career_p]

        player['career_reg_pitching'] = vals

        if len(post) > 0:
            df = pd.DataFrame(post,columns=self.p_labels)
            df.drop(['pid','name','year','team','Age','ERA','K9','BB9','HR9','tid','pos'], axis=1, inplace=True) #drop labels
            temp = df.sum(axis=0)
            vals = list(df.sum(axis=0).values)
            #era
            vals.append(float(temp['ER'])/float(max(temp['IP'],1))*9)
            #k9
            vals.append(float(temp['K'])/float(max(temp['IP'],1))*9)
            #bb9
            vals.append(float(temp['BB'])/float(max(temp['IP'],1))*9)
            #k9
            vals.append(float(temp['HRA'])/float(max(temp['IP'],1))*9)
            vals = [player['pid'],player['name'],player['team'],player['hof']] + vals

            intvals = ['W','L','S','ER','K','BB','HRA']
            d = dict(zip(self.career_p,vals))
            for i in intvals:
                d[i] = int(d[i])
            vals = [d[k] for k in self.career_p]


            player['career_post_pitching'] = vals

        return player

    
    def generate_player_dict(self,idnum,data):  

        categories = ['season','tid','gp','gs','fga','fg','ft','orb','blk','pts','stl','tp','ast','tov','errors']
        p_categories = ['season','tid','gp','gs','fta','winP','lossP','save','pf','fgAtRim','fgaAtRim','fgLowPost']

        player = {}
        player['pos'] = 'p' if data['players'][idnum]['offDefK']=='def' else 'f'
        player['name'] = data['players'][idnum]['name']
        player['awards'] = data['players'][idnum]['awards']
        player['award_dict'] = self.generate_awards(player['awards'])
        player['team'] = self.teams[data['players'][idnum]['tid']]
        player['active'] = data['players'][idnum]['active']
        player['hof'] = data['players'][idnum]['hof']
        player['born'] = data['players'][idnum]['born']['year']
        player['pid'] = data['players'][idnum]['pid']
        player['current_year'] = int(data['meta']['phaseText'][:4])
        reg = []
        post = []


        if 'stats' not in data['players'][idnum].keys():
            player['stats'] = False
            return player

        player['stats'] = True

        seasons = data['players'][idnum]['stats']
        reg_seas = [x for x in seasons if not x['playoffs']]
        post_seas = [x for x in seasons if x['playoffs']]

        #####################################################
        ################### HITTING #########################
        #####################################################

        reg = []
        post = []

        for seas in reg_seas:
            temp = [seas[x] for x in categories]

            #ba
            temp.append(float(temp[5])/float(max(temp[4],1)))
            #obp
            temp.append(float(temp[5]+temp[12])/float(max(temp[4],1)))
            #slg
            temp.append(float(temp[5]+temp[6]+2*temp[7]+3*temp[8])/float(max(temp[4],1)))
            #ops
            temp.append(float(temp[16])+float(temp[17]))

            temp.append(temp[0]-player['born'])
            temp[1] = self.teams[temp[1]]
            temp.append( self.tids[(temp[0],temp[1])])
            temp.append(player['pos'])
            temp = [player['pid'],player['name']] + temp
            reg.append(temp)

        for seas in post_seas:
            temp = [seas[x] for x in categories]
            #ba
            temp.append(float(temp[5])/float(max(temp[4],1)))
            #obp
            temp.append(float(temp[5]+temp[12])/float(max(temp[4],1)))
            #slg
            temp.append(float(temp[5]+temp[6]+2*temp[7]+3*temp[8])/float(max(temp[4],1)))
            #ops
            temp.append(float(temp[16])+float(temp[17]))

            temp.append(temp[0]-player['born'])
            temp[1] = self.teams[temp[1]]
            temp.append( self.tids[(temp[0],temp[1])])
            temp.append(player['pos'])
            temp = [player['pid'],player['name']] + temp
            post.append(temp)

        player['reg_batting'] = reg
        player['post_batting'] = post



        #####################################################
        ################### PITCHING ########################
        #####################################################
        reg = []
        post = []

        for seas in reg_seas:
            temp = [seas[x] for x in p_categories]
            #era
            temp.append(float(temp[8])/float(max(temp[4],1))*9)
            #k9
            temp.append(float(temp[9])/float(max(temp[4],1))*9)
            #bb9
            temp.append(float(temp[10])/float(max(temp[4],1))*9)
            #k9
            temp.append(float(temp[11])/float(max(temp[4],1))*9)
            temp.append(temp[0]-player['born'])
            temp[1] = self.teams[temp[1]]
            temp.append( self.tids[(temp[0],temp[1])])
            temp.append(player['pos'])
            temp = [player['pid'],player['name']] + temp
            reg.append(temp)

        for seas in post_seas:
            temp = [seas[x] for x in p_categories]

            #era
            temp.append(float(temp[8])/float(max(temp[4],1))*9)
            #k9
            temp.append(float(temp[9])/float(max(temp[4],1))*9)
            #bb9
            temp.append(float(temp[10])/float(max(temp[4],1))*9)
            #k9
            temp.append(float(temp[11])/float(max(temp[4],1))*9)

            temp.append(temp[0]-player['born'])
            temp[1] = self.teams[temp[1]]
            temp.append( self.tids[(temp[0],temp[1])])
            temp.append(player['pos'])
            temp = [player['pid'],player['name']] + temp
            post.append(temp)

        player['reg_pitching'] = reg
        player['post_pitching'] = post

        player = self.generate_career_stats(player)

        return player 