import time
import datetime
from dotenv import dotenv_values
from sqlalchemy import create_engine, text, MetaData, Table, ForeignKey, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, relationship
import pandas as pd
from tabulate import tabulate

# docs
# https://docs.sqlalchemy.org/en/14/orm/tutorial.html#querying

try:
    ENV_CONFIG = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}
except ImportError as e:
        raise ImportError('python-dotenv is not installed, run `pip install python-dotenv`') 

def output_table(titles = [], rows = []):
    """
    output_tables(['names', 'weights', 'costs', 'unit_costs'], [[xx, xx], [xx, xx]])
    """
    data = [titles] + list(zip(*rows))

    for i, d in enumerate(data):
        line = '|'.join(str(x).ljust(12) for x in d)
        print(line)
        if i == 0:
            print('-' * len(line))

def output_pandas(headers = [], data = []):
    print(pd.DataFrame(data, headers))

class Manager:

    def __init__(self):
        self.engine = create_engine('mssql+pymssql://{0}:{1}@{2}:{3}/{4}'.format(ENV_CONFIG["DBUSER"], ENV_CONFIG["DBPASS"], ENV_CONFIG["DBURL"], ENV_CONFIG["DBPORT"], ENV_CONFIG["DBNAME"]))
        self.metadata = MetaData()
        self.reflect_all_models()
        self.create_session()

    """Model and database methods"""
    def reflectModels(self):
        self.tbl_users = Table('User', self.metadata, autoload_with=self.engine)
        self.tbl_followers = Table('Follower', self.metadata, autoload_with=self.engine)
    
    def reflect_all_models(self):
        self.metadata.reflect(bind=self.engine)
        # we can then produce a set of mappings from this MetaData.
        self.Base = automap_base(metadata=self.metadata)
        # calling prepare() just sets up mapped classes and relationships.
        self.Base.prepare()

        # output classes
        """
        for mc in self.Base.classes.keys():
            print(mc)
        """

        self.tbl_user, self.tbl_follower, self.tbl_bmel, self.tbl_user_role = self.Base.classes.User, self.Base.classes.Follower, self.Base.classes.BinanceMirrorEventLogs, self.Base.classes.UserRole
    
    def setup_relations(self):
        self.tbl_bmel.user = relationship("User", order_by=self.tbl_user.Id, back_populates="BinanceMirrorEventLogs")
    
    def create_session(self):
        Session = sessionmaker(bind=self.engine)
        self.dbsession = Session()

    """Helper methods"""
    def formatDate(self, d):
        return d.strftime("%d.%m.%Y %H:%M:%S")

    def convertTotalPL(self, plval):
        return 100 + ((plval-1) * 100)

    def addPL(self, pla, plb):
        return ((pla/100) + 1) + ((plb/100)+1)

    """Actual data query methods"""
    def get_users_follow_allowed(self):
        if self.dbsession is not None:
            q = self.dbsession.query(self.tbl_user).\
                order_by(self.tbl_user.Id).\
                    filter(self.tbl_user.IsFollowingAllowed==True)
            results = q.all()
            print('{0} users are allowed to follow'.format(len(results)))

    def get_followers_of_trader(self, tradername):
        print('Followers of trader {0} ordered by follow date ascending:'.format(tradername))
        titles = ["Trader", "Follower", "Following since"]
        traders = []
        followers = []
        follow_dates = []
        follow_amts = []
        if self.dbsession is not None:
            trader_id = self.dbsession.query(self.tbl_user.Id).filter(self.tbl_user.UserName==tradername).first()
            for followingSince, followerName, followAmount in self.dbsession.query(self.tbl_follower.DateCreated, self.tbl_user.UserName, self.tbl_follower.FollowAmount).\
                join(self.tbl_user, self.tbl_follower.FollowedById==self.tbl_user.Id).\
                order_by(self.tbl_follower.DateCreated.asc()).\
                    filter(self.tbl_follower.Deleted==False).\
                    filter(self.tbl_follower.FollowedUserId==trader_id):
                traders.append(tradername)
                followers.append(followerName)
                follow_dates.append(self.formatDate(followingSince))
                follow_amts.append('${:.2f}'.format(followAmount))
                # print('{0} since {1}'.format(followerName, self.formatDate(followingSince)))
            # output_table(titles, [traders, followers, follow_dates])
            df = pd.DataFrame({'Trader': traders, 'Follower': followers, 'Following since': follow_dates, 'Follow Amount': follow_amts})
            print(tabulate(df,headers='keys'))

    def get_profitloss_alltime(self, username):
        print('All-time profit-loss of user {0}:'.format(username))
        users = []
        pls = []
        if self.dbsession is not None:
            user_id = self.dbsession.query(self.tbl_user.Id).filter(self.tbl_user.UserName==username).first()
            users.append(username)
            prevpl = 0
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            for plamount in self.dbsession.query(self.tbl_bmel.DerivedPositionLnGrowth).\
                filter(self.tbl_bmel.UserId==user_id).all():
                if prevpl == 0:
                    prevpl = plamount
                else:
                    prevpl = self.addPL(prevpl, plamount)
            pls.append('{:.2f}%'.format(prevpl))
            df = pd.DataFrame({'User': users, 'P/L': pls})
            print(tabulate(df,headers='keys'))

    def get_top_traders_volume(self, count):
        print('Get top {0} traders by trading volume:'.format(count))
        users = []
        volumes = []
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            for tvol, uname in self.dbsession.query(func.sum(self.tbl_bmel.DerivedTradeVolume).label('tradevol'), self.tbl_user.UserName).\
                join(self.tbl_user, self.tbl_bmel.UserId==self.tbl_user.Id).\
                join(self.tbl_user_role, self.tbl_user.Id==self.tbl_user_role.UserId).\
                filter(self.tbl_user_role.RoleId==3).\
                group_by(self.tbl_user.UserName).\
                order_by(func.sum(self.tbl_bmel.DerivedTradeVolume).desc()).all()[0:10]:
                users.append(uname)
                volumes.append('${:.2f}'.format(tvol))

            df = pd.DataFrame({'Trader': users, 'Trading Volume': volumes})
            print(tabulate(df,headers='keys'))

    def get_top_followers_volume(self, count):
        print('Get top {0} followers by trading volume:'.format(count))
        users = []
        volumes = []
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            for tvol, uname in self.dbsession.query(func.sum(self.tbl_bmel.DerivedTradeVolume).label('tradevol'), self.tbl_user.UserName).\
                join(self.tbl_user, self.tbl_bmel.UserId==self.tbl_user.Id).\
                join(self.tbl_user_role, self.tbl_user.Id==self.tbl_user_role.UserId).\
                filter(self.tbl_user_role.RoleId==4).\
                group_by(self.tbl_user.UserName).\
                order_by(func.sum(self.tbl_bmel.DerivedTradeVolume).desc()).all()[0:10]:
                users.append(uname)
                volumes.append('${:.2f}'.format(tvol))

            df = pd.DataFrame({'Follower': users, 'Trading Volume': volumes})
            print(tabulate(df,headers='keys'))


started_at = time.monotonic()

m = Manager()
# m.get_users_follow_allowed()
# m.get_followers_of_trader("Moneyguru")
# m.get_profitloss_alltime("Moneyguru")
m.get_top_traders_volume(10)

duration = time.monotonic() - started_at
duration_minutes = duration/60
print(f'Elapsed time: {duration_minutes:.2f} minutes')

"""
timeaxis = [*range(24)]
chartdata_trader = []
chartdata_followers = []
with engine.connect() as conn:
  t_user = conn.execute(text("select UserName from [User] where Id = {0}".format(trader_id)))
  trader = t_user.one()
  trader_result = conn.execute(text("select DATEPART(HOUR,ExchangeTimeStamp), sum(DerivedPositionLnGrowth) as 'plsum' from BinanceMirrorEventLogs where UserId = {0} group by DATEPART(HOUR,ExchangeTimeStamp) order by DATEPART(HOUR,ExchangeTimeStamp)".format(trader_id)))
  t_arr = []
  for t_row in trader_result:
    t_arr.append(t_row.plsum)
  chartdata_trader.append([trader.UserName, t_arr])
  result = conn.execute(text("select f.FollowedById, f.FollowedUserId, f.DateCreated, f.FollowAmount, u.UserName from Follower f join [User] u on u.Id = f.FollowedById where f.FollowedUserId = {0} and f.Deleted = 0".format(trader_id)))
  frows = result.all()
  for row in frows:
    print('Follower: {0}'.format(row.UserName))
    f_res = conn.execute(text("select DATEPART(HOUR,ExchangeTimeStamp), sum(DerivedPositionLnGrowth) as 'plsum' from BinanceMirrorEventLogs where UserId = {0} group by DATEPART(HOUR,ExchangeTimeStamp) order by DATEPART(HOUR,ExchangeTimeStamp)".format(row.FollowedById)))
    f_arr = []
    for f_resd in f_res:
      f_arr.append(f_resd.plsum)
    chartdata_followers.append([row.UserName, f_arr])
  # print(result.all())
  """