import sys, getopt
import time
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import create_engine, text, MetaData, Table, ForeignKey, func, or_, and_
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

        self.tbl_user, self.tbl_follower, self.tbl_bmel, self.tbl_user_role, self.tbl_kyc_attempts, self.tbl_wallet, self.tbl_transaction = self.Base.classes.User, self.Base.classes.Follower, self.Base.classes.BinanceMirrorEventLogs, self.Base.classes.UserRole, self.Base.classes.KycAttempts, self.Base.classes.Wallet, self.Base.classes.Transaction
    
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

    def formatNow(self):
        return datetime.now().strftime("%d.%m.%Y %H:%M")

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
                order_by(func.sum(self.tbl_bmel.DerivedTradeVolume).desc()).all()[0:int(count)]:
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
                order_by(func.sum(self.tbl_bmel.DerivedTradeVolume).desc()).all()[0:int(count)]:
                users.append(uname)
                volumes.append('${:.2f}'.format(tvol))

            df = pd.DataFrame({'Follower': users, 'Trading Volume': volumes})
            print(tabulate(df,headers='keys'))

    def get_top_traders_balance(self, count):
        print('Get top {0} traders by latest portfolio balance:'.format(count))
        users = []
        volumes = []
        balances = []
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            for usdtbalance, tradevol, uname in self.dbsession.query(func.max(self.tbl_bmel.DerivedUsdtValue).label('balance'), func.sum(self.tbl_bmel.DerivedTradeVolume).label('tradevol'), self.tbl_user.UserName).\
                join(self.tbl_user, self.tbl_bmel.UserId==self.tbl_user.Id).\
                join(self.tbl_user_role, self.tbl_user.Id==self.tbl_user_role.UserId).\
                filter(self.tbl_user_role.RoleId==3).\
                group_by(self.tbl_user.UserName).\
                order_by(func.max(self.tbl_bmel.DerivedUsdtValue).desc()).all()[0:int(count)]:
                users.append(uname)
                volumes.append('${:.2f}'.format(tradevol))
                balances.append('${:.2f}'.format(usdtbalance))

            df = pd.DataFrame({'Trader': users, 'Total Balance': balances, 'Trading Volume': volumes})
            print(tabulate(df,headers='keys'))

    def get_top_followers_balance_max(self, count):
        print('Get top {0} followers by max portfolio balance:'.format(count))
        users = []
        volumes = []
        balances = []
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            for usdtbalance, tradevol, uname in self.dbsession.query(func.max(self.tbl_bmel.DerivedUsdtValue).label('balance'), func.sum(self.tbl_bmel.DerivedTradeVolume).label('tradevol'), self.tbl_user.UserName).\
                join(self.tbl_user, self.tbl_bmel.UserId==self.tbl_user.Id).\
                join(self.tbl_user_role, self.tbl_user.Id==self.tbl_user_role.UserId).\
                filter(self.tbl_user_role.RoleId==4).\
                group_by(self.tbl_user.UserName).\
                order_by(func.max(self.tbl_bmel.DerivedUsdtValue).desc()).all()[0:int(count)]:
                users.append(uname)
                volumes.append('${:.2f}'.format(tradevol))
                balances.append('${:.2f}'.format(usdtbalance))

            df = pd.DataFrame({'Follower': users, 'Total Balance': balances, 'Trading Volume': volumes})
            print(tabulate(df,headers='keys'))

    def get_top_followers_balance(self, count):
        print('Get top {0} followers by latest portfolio balance:'.format(count))
        users = []
        balances = []
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            for usdtbalance, uname in self.dbsession.query(self.tbl_bmel.DerivedUsdtValue, self.tbl_user.UserName).\
                join(self.tbl_user, self.tbl_bmel.UserId==self.tbl_user.Id).\
                join(self.tbl_user_role, self.tbl_user.Id==self.tbl_user_role.UserId).\
                filter(self.tbl_user_role.RoleId==4).\
                group_by(self.tbl_user.UserName).\
                order_by(self.tbl_bmel.DateCreated.desc()).all()[0:int(count)]:
                users.append(uname)
                balances.append('${:.2f}'.format(usdtbalance))

            df = pd.DataFrame({'Follower': users, 'Latest Balance': balances})
            print(tabulate(df,headers='keys'))

    def get_cnt_user_with_withdrawals(self, suppress_action = False):
        if suppress_action == False:
            print('Get number of users that had at least one withdrawal already:')
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            num_of_users, = self.dbsession.query(func.count(self.tbl_user.Id)).\
                filter(
                    text("exists (select t2.Id from [Transaction] t2 join Wallet w2 on w2.Id = t2.WalletId where w2.UserId = [User].[Id] and t2.Amount < 0 and t2.TransactionType = 1)")
                ).first()

            print("{0} users already had at least one withdrawal as of {1}".format(num_of_users, self.formatNow()))

    def get_cnt_users_basisid_kyc(self, suppress_action = False):
        if suppress_action == False:
            print('Get number of users that completed BasisId KYC successfully:')
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            num_of_users, = self.dbsession.query(func.count(self.tbl_user.Id)).\
                filter(self.tbl_user.IsBasisKycDone==1).first()

            print("{0} users completed BasisId KYC successfully as of {1}".format(num_of_users, self.formatNow()))

    def get_cnt_users_basisid_kyc_with_balance(self, suppress_action = False):
        if suppress_action == False:
            print('Get number of users that completed BasisId KYC successfully with balance > 0 TYC:')
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            num_of_users, = self.dbsession.query(func.count(self.tbl_user.Id)).\
                filter(self.tbl_user.IsBasisKycDone==1).\
                filter(text("(select sum(t2.Amount) from [Transaction] t2 join Wallet w2 on w2.Id = t2.WalletId where w2.UserId = [User].[Id]) > 0")).first()

            print("{0} users with TYC balance > 0 completed BasisId KYC successfully as of {1}".format(num_of_users, self.formatNow()))

    def get_cnt_users_basisid_kyc_withdrawn_all(self, suppress_action = False):
        if suppress_action == False:
            print('Get number of users that completed BasisId KYC successfully and withdrew all their token:')
        if self.dbsession is not None:
            # for plamount, in self.dbsession.query(func.sum(self.tbl_bmel.DerivedPositionLnGrowth)).\
            num_of_users, = self.dbsession.query(func.count(self.tbl_user.Id)).\
                filter(self.tbl_user.IsBasisKycDone==1).\
                filter(text("(select sum(abs(t2.Amount)) from [Transaction] t2 join Wallet w2 on w2.Id = t2.WalletId where w2.UserId = [User].[Id] and t2.TransactionType = 1) = (select sum(abs(t2.Amount)) from [Transaction] t2 join Wallet w2 on w2.Id = t2.WalletId where w2.UserId = [User].[Id] and t2.TransactionType <> 1 and t2.TransactionType <> 2)")).first()

            print("{0} users completed BasisId KYC successfully and withdrew everything as of {1}".format(num_of_users, self.formatNow()))

    def get_sum_unlocked_tyc_wallets(self, suppress_action = False):
        if suppress_action == False:
            print('Get sum of all unlocked TYC currently in wallets:')
        if self.dbsession is not None:
            # need to customize to respect vesting rules for other wallet types later
            num_of_users, = self.dbsession.query(func.sum(self.tbl_transaction.Amount)).\
                filter(or_(self.tbl_transaction.WalletType==1, self.tbl_transaction.WalletType==0)).first()

            print("{0} TYC in total are unlocked available in wallets as of {1} (query to be refined)".format(num_of_users, self.formatNow()))

    def get_cnt_users(self, suppress_action = False):
        if suppress_action == False:
            print('Get count of all verified users:')
        if self.dbsession is not None:
            # need to customize to respect vesting rules for other wallet types later
            num_of_users, = self.dbsession.query(func.count(self.tbl_user.Id)).\
                filter(self.tbl_user.Deleted==0).\
                filter(self.tbl_user.EmailConfirmed==1).first()

            print("{0} verified users as of {1}".format(num_of_users, self.formatNow()))

    def get_last_activity(self, username, suppress_action = False):
        if suppress_action == False:
            print('Get last activity of user {0}'.format(username))
        if self.dbsession is not None:
            user_id = self.dbsession.query(self.tbl_user.Id).filter(self.tbl_user.UserName==username).first()
            for exchangetimestamp, usdtval, growth in self.dbsession.query(self.tbl_bmel.ExchangeTimeStamp, self.tbl_bmel.DerivedUsdtValue, self.tbl_bmel.DerivedPositionLnGrowth).\
                filter(self.tbl_bmel.UserId==user_id).\
                order_by(self.tbl_bmel.ExchangeTimeStamp.desc()).all()[0:1]:
                print('Activity Date: {0} Portfolio USDT: ${1:.2f} PositionLnGrowth: {2}%'.format(self.formatDate(exchangetimestamp), usdtval, growth))


    def get_slt_general_status(self):
        self.get_cnt_users(True)
        self.get_cnt_users_basisid_kyc(True)
        self.get_cnt_users_basisid_kyc_with_balance(True)
        self.get_cnt_user_with_withdrawals(True)
        self.get_cnt_users_basisid_kyc_withdrawn_all(True)
        self.get_sum_unlocked_tyc_wallets(True)

    def supp_check(self, email):
        print('General checkup of email {0}:'.format(email))
        if self.dbsession is not None:
            userobj = self.dbsession.query(self.tbl_user).filter(self.tbl_user.Email==email).first()
            # existing
            if userobj is not None:
                print('User "{0}"" exists.'.format(userobj.UserName))
            else:
                print('No user for email "{0}"'.format(email))
                return
            # Email verified
            if userobj.EmailConfirmed:
                print('Email verified: yes')
            else:
                print('Email verified: no')
                return
            # get role
            user_role = self.dbsession.query(self.tbl_user_role.RoleId).filter(self.tbl_user_role.UserId==userobj.Id).first()
            print('Role: {0}'.format('Trader' if user_role == 3 else 'Follower'))
            # has old KYC
            if userobj.IsKycDone:
                print('Has old KYC: yes')
            else:
                print('Has old KYC: no')
            # has basisId kyc
            if userobj.IsBasisKycDone:
                print('Has BasisId KYC: yes')
            else:
                print('Has BasisId KYC: no')
                # now check if our common KYC error is there
                num_success_attempts, = self.dbsession.query(func.count(self.tbl_kyc_attempts.Id)).\
                    filter(self.tbl_kyc_attempts.UserId == userobj.Id).\
                    filter(self.tbl_kyc_attempts.BasisIdStatus == 10).first()
                if num_success_attempts >= 1:
                    print('Has KYC issue (status 10 but not set on user record)')
            # no of kyc attempts
            kyc_attempts_cnt = self.dbsession.query(self.tbl_kyc_attempts).filter(self.tbl_kyc_attempts.UserId==userobj.Id).count()
            print('KYC Attempts in total: {0}'.format(kyc_attempts_cnt))
            # token balance


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

def main(argv):
    started_at = time.monotonic()
    m = Manager()

    try:
        opts, args = getopt.getopt(argv, "hm:p:",["method=","parameters="])
    except getopt.GetoptError:
        print('tyc.py -m support_analyze -p 3310')
        sys.exit(2)

    userargs = None

    for opt, arg in opts:
        if opt == "-h":
            print('tyc.py -m support_analyze -p 3310')
            sys.exit()
        elif opt in ("-m", "--method"):
            methodname = arg
        elif opt in ("-p", "--parameters"):
            userargs = arg

    if userargs:
        getattr(m, methodname)(userargs)
    else:
        getattr(m, methodname)()

    # m.get_users_follow_allowed()
    # m.get_followers_of_trader("Moneyguru")
    # m.get_profitloss_alltime("Moneyguru")
    # m.get_top_traders_volume(10)
    # m.get_top_followers_volume(10)
    # m.get_top_traders_balance(10)
    # m.get_top_followers_balance(10)

    duration = time.monotonic() - started_at
    duration_minutes = duration/60
    print(f'Elapsed time: {duration_minutes:.2f} minutes')

if __name__ == "__main__":
   main(sys.argv[1:])