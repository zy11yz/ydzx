---处理手机档次


U = load '$SEGMENT' using PigStorage('\t') as (segment:chararray, userid:chararray);
D = load 'yidian.profile_user_v2' using org.apache.hive.hcatalog.pig.HCatLoader();
D = filter D by (p_day == '$PDAY' and p_type == 'full');
G = join U by userid left outer,D by user_id;
G = foreach G generate segment,userid,dims;
G = filter G by (dims is not null);
H = foreach G generate segment,userid,flatten(dims) as dim;
I = filter H by (STARTSWITH(dim,'demo:dlevel'));
I = distinct I;
J = group I by (segment,dim);
DLEVEL = foreach J generate group.segment,'dlevel',(chararray)group.dim as detail,COUNT(I.userid);
store DLEVEL into '$OUTPUT';
