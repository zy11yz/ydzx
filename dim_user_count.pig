U = load '$SEGMENT' using PigStorage('\t') as (segment:chararray, userid:chararray);
--U = load '/user/liuyichao/test/adhoc/' using PigStorage('\t') as (segment:chararray, userid:chararray);
D = load 'working_dw.dim_user_info' using org.apache.hive.hcatalog.pig.HCatLoader();
D = filter D by (p_day == '$PDAY');
--D = filter D by (p_day == '2015-08-20');
G = join U by userid left outer,D by user_id;
G = foreach G generate segment,userid,city,province,region,depth,age,gender,platform;
G = filter G by((segment is not null)and(userid is not null)and((city is not null) or(province is not null)or(region is not null)or(depth is not null)or(age is not null)or(gender is not null)or(platform is not null)));
G = distinct G;
--store G into '$OUTPUT';



G_CITY2 = filter G by (city is not null);
G_CITY = group G_CITY2 by (segment,city);
G_CITY = foreach G_CITY generate group.segment,'city' as type,group.city as detail,COUNT(G_CITY2.userid) as cnt;


G_PROVINCE2 = filter G by (province is not null);
G_PROVINCE = group G_PROVINCE2 by (segment,province);
G_PROVINCE = foreach G_PROVINCE generate group.segment,'province' as type,group.province as detail,COUNT(G_PROVINCE2.userid) as cnt;

G_REGIN2 = filter G by (region is not null);
G_REGIN = group G_REGIN2 by (segment,region);
G_REGIN = foreach G_REGIN generate group.segment,'regin' as type,group.region as detail,COUNT(G_REGIN2.userid) as cnt;

G_DEPTH2 = filter G by (depth is not null);
G_DEPTH = group G_DEPTH2 by (segment,depth);
G_DEPTH = foreach G_DEPTH generate group.segment,'depth' as type,(chararray)group.depth as detail,COUNT(G_DEPTH2.userid) as cnt;

G_AGE2 = filter G by (age is not null);
G_AGE = group G_AGE2 by (segment,age);
G_AGE = foreach G_AGE generate group.segment,'age' as type,(chararray)group.age as detail,COUNT(G_AGE2.userid) as cnt;

G_GENDER2 = filter G by (gender is not null);
G_GENDER = group G_GENDER2 by (segment,gender);
G_GENDER = foreach G_GENDER generate group.segment,'gender' as type,(chararray)group.gender as detail,COUNT(G_GENDER2.userid) as cnt;

G_PLATFORM2 = filter G by (platform is not null);
G_PLATFORM = group G_PLATFORM2 by (segment,platform);
G_PLATFORM = foreach G_PLATFORM generate group.segment,'platform' as type,(chararray)group.platform as detail,COUNT(G_PLATFORM2.userid) as cnt;

C_STORE = union G_CITY,G_PROVINCE;
C_STORE = union C_STORE,G_REGIN;
C_STORE = union C_STORE,G_DEPTH;
C_STORE = union C_STORE,G_AGE;
C_STORE = union C_STORE,G_GENDER;
C_STORE = union C_STORE,G_PLATFORM;


store C_STORE into '$OUTPUT';



