register 'yidian_udf.py'  using jython as yidian_udf;
register 'cf_udf.py'  using jython as cf_udf;

%default NTASK 39

%default CONDITION ((facet=='cs_ct')or(facet=='cs_sct')or(facet=='cs_fromid')or(facet=='cs_keyword')or(facet=='ens_gender')or(facet=='xiaomi_age'))

%default SCONDITION (1==1)

D = load 'yidian.profile_user_v2' using org.apache.hive.hcatalog.pig.HCatLoader();
D = filter D by (p_day == '$PDAY' and p_type == 'full');

-- D: {user_id: chararray,demo: (create_date: chararray,app_id: chararray,weibo_id: chararray,xiaomi_id: chararray,fromid_cnt: int,selffromid_cnt: int,device_name: chararray,geo: chararray,os: chararray,distribution_channel: chararray),user_headfea_on: {innertuple: (facet: chararray,interest: chararray,mean: double,variance: double,poscnt: double,negcnt: double,update_date: chararray)},user_headfea_off: {innertuple: (facet: chararray,interest: chararray,mean: double,variance: double,poscnt: double,negcnt: double,update_date: chararray)},dims: {innertuple: (innerfield: chararray)},p_day: chararray}

D = filter D by ((user_headfea_off is not null) and (not IsEmpty(user_headfea_off)));

D = foreach D {
    B = filter user_headfea_off by ($CONDITION);
    generate user_id as userid, B;
}

D = filter D by (not IsEmpty(B));

-- W = foreach D generate userid, flatten(B.(facet,interest,mean)) as (facet, interest, mean);

U = load '$SEGMENT' using PigStorage('\t') as (segment:chararray, userid:chararray);
G = group U by segment PARALLEL 9;
U = foreach G generate flatten(U), (double)COUNT(U.userid) as count:double;
U = filter U by ($SCONDITION);

G = cogroup D by userid, U by userid inner PARALLEL 179;
F = foreach G generate flatten(U.(segment, userid, count)), flatten(D.B) as B;

W = foreach F generate segment, userid, count, flatten(B.(facet,interest, mean)) as (facet, interest, mean);

G = group W by (segment, facet, interest) PARALLEL 179;
S = foreach G {
    I = limit W 1;
    generate flatten(group) as (segment, facet, interest), SUM(W.mean) as acc, flatten(I.count) as count;
}
S = foreach S generate segment, facet, interest, (acc / count) as mean, 0.0 as var, acc as pos, count as count;
S = order S by segment, facet, mean DESC PARALLEL $NTASK;

store S into '$OUTPUT';

