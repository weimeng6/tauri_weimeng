use anyhow::{anyhow, Context, Result};
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, CONNECTION, ORIGIN, REFERER, USER_AGENT};
use serde_json::{json, Value};
use std::collections::HashMap;
use chrono::{NaiveDate,Duration};
use tokio;

struct Apps {
    url: String,
    ku: String,
}
impl Apps {
    fn new(url: &str, ku: &str) -> Self {
        Apps {
            url: url.to_string(),
            ku: ku.to_string(),
        }
    }
    fn extract_host_id(&self) -> Result<(String, String)> {
        // 匹配主机部分（非贪婪匹配直到 /#）
        let host_re = Regex::new(r"http://(.*?)/#")?;
        let host = host_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("URL 格式错误，无法提取主机"))?;

        // 匹配 ID 部分
        let id_re = Regex::new(r"id=([^&]*)")?;
        let id = id_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("URL 格式错误，无法提取 ID"))?;

        Ok((host, id))
    }

    fn head(&self) -> Result<(HeaderMap, String, String, String)> {
        let (host, id) = self.extract_host_id()?;

        // 构建 URL
        let url0 = format!("http://{}/WebDb/sql/parse", host);
        let url1 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=true", host, id, self.ku);
        let url2 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=&length=1000", host, id, self.ku);

        // 构建请求头
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json, text/plain, */*"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9,ja;q=0.8"));
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ORIGIN, HeaderValue::from_str(&format!("http://{}", host))?);
        headers.insert(REFERER, HeaderValue::from_str(&format!("http://{}/", host))?);
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
        );

        Ok((headers, url0, url1, url2))
    }

    async fn get_data(&self, sql: &str) -> Result<Vec<HashMap<String, Value>>> {
        let (headers, url0, url1, url2) = self.head()?;
        let client = reqwest::Client::new();


        // 第一个请求 (url0)
        let data0 = json!({"sql": sql.replace("\n"," "), "type": "sql"});
        let _res0 = client
            .post(&url0)
            .headers(headers.clone())
            .json(&data0)
            .send()
            .await
            .context("URL0 请求失败")?;

        // 第二个请求 (url1)
        let data = json!({"sql": sql.replace("\n"," ")});
        let _res1 = client
            .post(&url1)
            .headers(headers.clone())
            .json(&data)
            .send()
            .await
            .context("URL1 请求失败")?;

        // 第三个请求 (url2)
        let res2 = client
            .post(&url2)
            .headers(headers)
            .json(&data)
            .send()
            .await
            .context("URL2 请求失败")?;

        // 解析响应
        let response_text = res2.text().await.context("无法获取响应文本")?;

        let json_response: Value = serde_json::from_str(&response_text).context("无法解析JSON响应")?;

        // 按原Python逻辑提取数据
        let result = json_response["data"]
            .as_array()
            .ok_or_else(|| anyhow!("响应中缺少 'data' 字段或不是数组"))?
            .get(0)
            .ok_or_else(|| anyhow!("'data' 数组为空"))?
            .get("data")
            .ok_or_else(|| anyhow!("缺少二级 'data' 字段"))?
            .get("data")
            .ok_or_else(|| anyhow!("缺少三级 'data' 字段"))?
            .as_array()
            .ok_or_else(|| anyhow!("'data' 字段不是数组"))?
            .iter()
            .map(|item| {
                let row = item.as_object().unwrap();
                row.iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect::<HashMap<String, Value>>()
            })
            .collect();

        Ok(result)
    }
}

pub struct GetData {
    start_date: String,
    end_date: String,
    jour_url : String,
    juhe_url : String,
    user_id_str1 : String,
    user_id_str2 : String,

}
impl GetData {
    pub fn new(start_date: &str, end_date: &str,jour_url:&str,juhe_url:&str) -> Self {

        GetData { start_date:start_date.to_string(),
            end_date:end_date.to_string(),
            jour_url:jour_url.to_string(),
            juhe_url:juhe_url.to_string() ,
            user_id_str1 : String::new(),
            user_id_str2 : String::new(),
        }
    }

    async fn get_num(&self) -> Result<HashMap<String,String>,Box<dyn std::error::Error>> {

        let sql= format!("select count(distinct(USER_ID)) as num   from tf_b_trade_2025 where trade_type_code = '3020' and accept_date > '{0} 00:00:00'
and accept_date < '{1} 00:00:00' and SUBSCRIBE_STATE = '9' union all

select count(distinct(USER_ID)) as num  from tf_b_trade_2025 where trade_type_code = '3011' and accept_date > '{0} 00:00:00'
and accept_date < '{1} 00:00:00' and SUBSCRIBE_STATE = '9'   union all

select count(distinct(USER_ID)) as num   from tf_b_trade_2025 where trade_type_code = '3012' and accept_date > '{0} 00:00:00'
and accept_date < '{1} 00:00:00' and SUBSCRIBE_STATE = '9';",&self.start_date,&self.end_date);
        let app=Apps::new(&self.jour_url,"jour");
        let data = app.get_data(&sql).await?;
        let mut nums_dict = HashMap::new();
        nums_dict.insert("自动续订数量".to_string(), data[0].get("NUM").and_then(Value::as_str).unwrap_or("").to_string() );
        nums_dict.insert("到期暂停数量".to_string(), data[1].get("NUM").and_then(Value::as_str).unwrap_or("").to_string() );
        nums_dict.insert("到期退订数量".to_string(), data[2].get("NUM").and_then(Value::as_str).unwrap_or("").to_string() );
        Ok(nums_dict)
    }
    async fn get_renew(&self) -> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>> {
        let sql= format!("select a.user_id, b.product_id,max(b.end_date) as end_date,a.cust_id,a.user_state,a.remove_tag  from tf_f_user a
inner join tf_f_user_product b
on a.user_id=b.user_id
where (a.user_tag_set not like '%4%' or a.user_tag_set is null) and a.remove_tag='0' and a.NET_TYPE_CODE = '00'
group  by a.user_id
having max(b.end_date) between '{} 00:00:00' and '{} 00:00:00';",&self.start_date,&self.end_date);
        let app=Apps::new(&self.juhe_url,"crm");
        let data = app.get_data(&sql).await?;
        Ok(data)

    }
    async fn get_destroy(&self) -> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>> {
        let date1 = NaiveDate::parse_from_str(&self.start_date, "%Y-%m-%d")? - Duration::days(16);
        let date2 = NaiveDate::parse_from_str(&self.end_date, "%Y-%m-%d")? - Duration::days(16);

        let sql= format!("select a.user_id, b.product_id,max(b.end_date) as end_date,a.cust_id,a.user_state,a.remove_tag  from tf_f_user a
inner join tf_f_user_product b
on a.user_id=b.user_id
where a.user_state<>'9' and a.remove_tag='0' and a.NET_TYPE_CODE = '00'
group  by a.user_id
having max(b.end_date) between '{} 00:00:00' and '{} 00:00:00' ;",date1,date2);
        let app=Apps::new(&self.juhe_url,"crm");
        let data = app.get_data(&sql).await?;
        Ok(data)

    }

    async fn if_renew(&self,user_ids:String,result1:Vec<HashMap<String, Value>>) -> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>> {
        let sql= format!("select a.USER_ID from tf_f_user_attr a
inner join (select user_id,max(end_date) as end_date from tf_f_user_attr where ATTR_CODE='autoRenew' and ATTR_VALUE='1'
and user_id in (
{}
) group by user_id) b
on a.user_id=b.user_id and a.end_date=b.end_date
where a.ATTR_CODE='autoRenew' and a.ATTR_VALUE='1' ;",user_ids);
        let app=Apps::new(&self.juhe_url,"crm");
        let data = app.get_data(&sql).await?;
        let user_id_renew:Vec<String> = data.iter().map(|x| x.get("USER_ID").and_then(Value::as_str).unwrap_or("").to_string()).collect();
        let result1 = result1.into_iter()
            .map(|mut x| {
                let status = match x.get("USER_ID") {
                    Some(Value::String(id)) if user_id_renew.contains(id) => "开启",
                    _ => "没有",
                };

                x.insert("if_renew".to_string(), json!(status));
                x
            })
            .collect();
        Ok(result1)


    }
    async fn get_version(&self,user_ids:String,type1:&str,result1:Vec<HashMap<String, Value>>) -> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>> {
        let date1 = &self.end_date[0..7].replace("-", "");
        let sql= format!("select *
from tf_b_deal_buffer_{0} a
         join (select deal_id, max(update_time) as update_time
               from tf_b_deal_buffer_{0}
               where DEAL_ID in (
                {1}
                   )
                 and DEAL_TYPE = '{2}'
               group by deal_id) as b on a.deal_id = b.deal_id and a.update_time = b.update_time and
                                         a.DEAL_TYPE = '{2}';",date1,user_ids,type1);
        let app=Apps::new(&self.jour_url,"jour");
        let data = app.get_data(&sql).await?;
        let result: HashMap<String, String> = data.iter().filter_map(|v| {
            let user_id = v.get("DEAL_ID")?.as_str()?.to_string();
            let desc = v.get("DEAL_DESC")?.as_str()?.to_string();
            Some((user_id, desc))
        }).collect();

        let result1 = result1.into_iter()
            .map(|mut x| {
                let status = match x.get("USER_ID") {
                    Some(Value::String(id)) => result.get(id).map(|s| s.as_str()).unwrap_or("没有"),
                    _ => "没有",
                };

                x.insert("aee_err".to_string(), json!(status));
                x
            })
            .collect();

        Ok(result1)


    }
    async fn if_ceshi(&self,user_ids:String,result1:Vec<HashMap<String, Value>>)-> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>>{
        let long: HashMap<String, String> = [
            ("66241b9d87fa464ab86707526cb9dced", "云网管"),
            ("0fdc97d4a2424eaf95eef0354dc52400", "云客服（cec）"),
            ("4bc5ca94cc0943aa9bc8f3fda2b547ba", "云安卫士"),
            ("070d4390bec54c5783adf4afedb22c99", "数据可视化DataInsight"),
            ("21460d3654374e93b954bd3d75b634b0", "乾坤大数据"),
            ("39d88250e784417f3844780e7c9c13f5", "云专线（子产品）"),
            ("064c8a791330e33ffad24e729b260ad8", "云桌面（GPU版、旗舰版)"),
            ("34e5ba05155341279e0e444778663e24", "云甲安全防护-Web全栈防护-上海安服"),
            ("6e0d69cb5673444fa7d6487b00f450b8", "富媒体内容审核"),
            ("92021e66fa744019b283079e9fc4d1f4", "5G云广播"),
            ("62f6534ca3659f37076f3b5924f128c3", "行车卫士"),
            ("0c441c8aa1484cf6a1f93427dc301ac1", "大云云盒"),
            ("a3cfffa0c161476e3bb0da1236f45c61", "专属云桌面"),
            ("7ef23f162708e814d71a84e2547d1a15", "专属云桌面（人天服务）"),
            ("101a5140acac4e335ef6802efd56e0ab", "云桌面带宽"),
            ("37658b432fa8f01f0860fb7b89244fdb", "云桌面-GPU版-显存"),
            ("49dd0465038c825c461d6f0ff26c9839", "云桌面_瘦终端"),
            ("c4eaa7f4b5c94cbcdab55ada01b2834c", "云桌面镜像"),
            ("6ce1f820646fe1a381435ca72a53130f", "云桌面_数据盘"),
            ("d22d9f6d7e7e4341a99b6a6c015c26f8", "移动云云桌面"),
            ("846de383c8504123b4039ac22378d066", "移动云云桌面带宽"),
            ("0cee3acb6eca4d08658503bdd92c51ce", "车务通"),
            ("d78e416809708227c0e0e195cc0e0076", "智能车载服务"),
            ("611089417292474a858240a974a8ef2e", "逍遥旅游"),
            ("9352fbcaffa61248ff2c6028d5eacdf4", "云端口"),
            ("0196b5422b3da9e51dced8cb11994b9d", "网宿云专线（SD-WAN）"),
            ("084b4ca322124ec2a48e2354d8bb0c02", "云专线（省内）"),
            ("5f0f7ea7bfa64fec9f54a1398fae92bb", "云专线（省云专网）"),
            ("7f5260e48adc45e2a54f53878c40f2de", "云专线（省云专网）"),
            ("b4d71772b101454cab564806426eaa33", "云专线（省间）"),
            ("e5bfe2e06e7b4beb917774864a895e3d", "云网一体（云专线）"),
            ("e69b0b3c1ab7472e9681a64f11df2879", "云专线（省云专网）"),
            ("5c0ac8d3bd5441d7aff9e1423a4d1f25", "超融合专属版云空间"),
            ("f79ee401f2ee40a4b01146147fe2a017", "人口统计大数据分析"),
            ("4e29e42b4aa042f5bf079f5a23ecb2da", "火瞳智慧通行"),
            ("8cb015654dc14579ac19f147350817f6", "神眼视频解析"),
            ("1c2da441c1084d4286e98627d9d6c4cf", "企业应用开发平台"),
            ("0f6b6fa8a0c04d109346915276f4bbb4", "区块链服务平台"),
            ("3cf6c4e48d5f4c8586766dd908f49d24", "云无线"),
            ("f20ee401f2ee40a4b011aemop9201100", "数据库审计"),
            ("751f3d03e8b34c5d8cc535a99718fbd3", "政务洞察"),
            ("55d8715bc87f4e9eaa91f383420ff56c", "云甲安全防护-Web全栈防护-甘肃安服"),
            ("5a9c484451a93ad35e98d1bbabbdf665", "云甲安全防护-Web全栈防护-北京"),
            ("6525f90280f24d19beb18076cbc4a139", "云甲安全防护-Web全栈防护-四川安服"),
            ("6629ce293ae4162db129eca0cb502c71", "云甲安全防护-Web全栈防护-江苏 广东"),
            ("96962c4c2fca4314be2635731be9d779", "云甲安全防护-Web全栈防护-北京2（清洗）"),
            ("eda7d6d7aaa8473f94908566b3042c48", "云甲安全防护-Web全栈防护-北京2（黑洞）"),
            ("08d110ee2a6134d2150214b995318413", "Web全栈防护黑洞"),
            ("0249c4286d8a466793cca272ef80876d", "语音验证码"),
            ("634b68d9307243fdafda44bb7fdd4695", "语音扩展"),
            ("a898777841e44a9590e2a340ca58456b", "语音通知"),
            ("ab55b2e8c2134b02b135b7d2e77c0965", "语音识别"),
            ("f8ad16a0399441f3a635e4fd12a8f8e9", "语音合成"),
            ("811f36c2b5e34f85939300dd12b1729a", "域名注册"),
            ("c9c3b914ed5a4935b797ff36a816a4e0", "域名注册"),
            ("81641f878eafba3e0571c77154e1d8b4", "无人机巡检安防系统"),
            ("2444c0d317c044748d4e57e44ab24e9f", "模板短信"),
            ("855790e98548eb6388e28cbae2628b6d", "中移舆情"),
            ("948e6d506bfc47f4b2ce772376623ed1", "乡村振兴平台"),
            ("38cfb4f6ea67447d8b904efebad2f6ae", "云安全中心-吉林版"),
            ("e5bd366c267d40348e44ef49902a9e96", "云组网（云网一体）"),
            ("2c90ff426e25b5641a4f9aa4b64bb183", "车务通"),
            ("4171014093024767b1f85633fecbca52", "大云云盒与云主机商品组合"),
            ("4914fcdcec484142a4aec38accdd644a", "云端口-新资费规格"),
            ("50aefb220c944a328efbbaca1a74af45", "大云云盒与云主机商品组合"),
            ("77e8f99a2a0aff08580d312b353a48e5", "省内专线（子产品）"),
            ("be7360ff52f3a829f115e72d64223cd9", "大云云盒云主机"),
            ("c36a4094655b424d57a6b88320cf7e5c", "行车卫士"),
            ("f79ee401f2ee40a4b01146147fe2a006", "离线语音听写"),
            ("f79ee401f2ee40a4b01146147fe2a007", "语音唤醒"),
            ("f79ee401f2ee40a4b01146147fe2a008", "离线命令词识别"),
            ("f79ee401f2ee40a4b01146147fe2a010", "离线语音合成"),
            ("fe784a95c7984f0ca973599a449191aa", "云组网"),
            ("mo7360ff52f3a829f115e72d64223cd9", "大云云盒云主机"),
            ("091912a246f54f238c5ccca34bfce756", "近源抗D-基础压制（包周期）"),
            ("1658cfa27e4d4eb380b4bfdd2446cde9", "近源抗D-流量清洗（按量）"),
            ("210ffed69a4f49c19654d8a935dd30e2", "近源抗D-标准压制（包周期）"),
            ("332aae594edf4b19be6d3c295066e4f3", "近源抗D-基础压制（按量）"),
            ("5759b385079940da91d8d8b73e379b33", "数据可视化DataInsight"),
            ("71bb3ee9e59f49c79d599d985b09a5c6", "近源抗D-一次性开通服务"),
            ("7894f0e85fbb49508106df04a5a99acd", "云无线"),
            ("78bb0e0416de43158a3f6aaee4aef464", "近源抗D-基础压制（一次性）"),
            ("8b9196cd233544358e9c86e7eceda638", "近源抗D-流量清洗（一次性）"),
            ("a990742d7a40448c8ac6855caa978a88", "近源抗D-标准压制（一次性）"),
            ("b1f5a700bdc4476cb0e6e1d5614e40f7", "近源抗D-分析溯源服务"),
            ("cff133092a2d420ea3b375856da5a953", "安全服务（福建版）"),
            ("d976749ea7534905b4374a84ef5f69b2", "近源抗D-流量清洗（包周期）"),
            ("dc425e79e9c44994870f697bda59467d", "近源抗D-流量监控服务（包周期）"),
            ("eff6b39ecd974b5fa10b774cf79c71a9", "近源抗D-标准压制（按量）"),
            ("f0f02fb849ef47ceb95be3c4bfdda5d0", "近源抗D-资源占用服务"),
            ("ff36b4d0e9ce8dbbe6bd4e94c1227875", "实人认证"),
            ("50369c30cc2c41d4b3663178b90e305b", "客群洞察"),
            ("ffffa4939db741fbb8e6007e5ef6b145", "态势感知-物联网版"),
            ("0df44c886c90436a95fb54c683fd9e70", "迁云服务"),
            ("7ba0ea73fe4b48159a15ce892729ba2c", "云安全资源池"),
            ("712862015b0646e190cb06acfc3ce671", "区块链服务平台：公证链"),
            ("021a2330fddc4f1d8e9e07237114f3fc", "磐匠数智员工"),
            ("a0ff09fa5f89445f9b286c9c9783de45", "SASE VPN"),
            ("19c5db35a2f6481a80d672197fbd43b4", "铁通安全资源池"),
            ("2e727c9d2f0a4cf79c68f4878b8680e8", "边缘智能小站"),
            ("1db89692bceb478d9a5687ee705f37f6", "数字广告营销服务"),
            ("9dfade2d1a914d3e9c45b2d03d610989", "云游戏服务平台"),
            ("30f1afe85eef48499e156f52683c34ea", "云电脑一体化支撑服务包"),
            ("3c22fe099ac14815a6e0fc31584af51a", "云安全中心-山东版"),
            ("bc2c91949d934a0dada142d8374b9f62", "中国移动CDN"),
            ("84ab6914dc234ae79ba4353abba8ea4c", "云电脑（政企版）"),
            ("b684269c4d574a24a758bedca575237f", "移动云云电脑"),
            ("322caed14e8e488db3881c7e2616d895", "云备份CBR（云融省版本）"),
            ("9deb59b069e74d589442415e2b6596fc", "云备份CBR（专业版）"),
            ("104246d7079547b69f3fc0472852f5b8", "混合云容灾（云融省版本）"),
            ("c5f157d4846ffb80cc8a44444484a48a", "视频直播（音乐云服务）"),
            ("a968871378c88021f3454c15ba246f7b", "云数据库MySQL"),
            ("e10a579d7aa0fcb5297fca0ad17017e8", "云甲安全防护-广东省清洗"),
            ("ef522b720fe35507f6eea4ce1d56d96c", "视频安防"),
            ("67bb53d1f93e46c18e2d22c05a5908b3", "安全资源池中移集成版"),
            ("1175f440c58740cebaedff08115d5f60", "云电脑网络包"),
            ("e33b4155fd444c7da296760251f16127", "云电脑（行业定制版）"),
            ("dc6393d3c657451db3cfbfd2d8e8ffec", "云无线"),
            ("9e50c3e9190a443498fbf5e163ae6a8c", "攻防演练"),
            ("3f92c3acf6aa48c6883f4a80e8618c38", "安全风险评估"),
            ("2f953c7e062a4b908e28ea7f23a23e42", "等保合规"),
            ("13171416d59742058f5e3f06b6715a20", "数据安全服务"),
            ("4567d206fb0140e9aa1b78fa9bac004c", "云身份安全服务-企业型（线下交付）"),
            ("e654ebb1b92749b6b0a564e885af56f7", "神眼视频解析"),
            ("5cc83d7fe8d7b27524c88b9e594638fc", "增强漏洞扫描"),
            ("becc385d87ff46d6a1808d9f0133d7bd", "数据集成与治理"),
            ("37114300952f4b0f9e7af7beba3d8dc7", "通算云主机"),
            ("e2cf4e95f4fd467aa5cbd120be69bebe", "（通算）容量型云硬盘 "),
            ("a8a4e6e4602c415b8c54cfeaad1a4730", "（通算）高性能型云硬盘"),
            ("b550bce48b5f44c282d339dc412c4e1a", "（通算）性能优化型云硬盘"),
            ("f3362906eb534ea2b9a89a9ba159cb80", "（通算）专享型云硬盘"),
            ("60c6aa59e342491f8989ececa9fcd5a3", "（上海）裸金属服务器"),
            ("9f3bfb976012465ebceed3c960f23758", "（通算）专享型文件存储"),
            ("b222fe0322614f52a5f4845c661eb2f4", "（通算）性能型文件存储"),
            ("48d27e46fbff4726bed0a42ae9be1fa8", "（通算）标准型文件存储"),
            ("f80cba300f814dea86dd13880073aa37", "（通算）极速型文件存储"),
            ("9ca8a9f4df856247e033e5e6c189efd5", "云数据库redis"),
            ("17c7b0c2a2f14691b36d68de10cb1cdd", "（黑龙江）裸金属服务器"),
            ("363f95f84f154882a867b18bce1f86d5", "（山西）裸金属服务器"),
            ("5a9ebf66b9574b5e9a7e6a81656f0805", "（河北）裸金属服务器"),
            ("b263ab604884410c8d6bdc91736394e3", "（北京）裸金属服务器"),
            ("d824df91c30547be8a9e8628fd1e4d40", "（内蒙古）裸金属服务器"),
            ("eed60c8f986042b49e4b9a51ce354a55", "（辽宁）裸金属服务器"),
            ("6fc8ad2c106741d19e5bf6da201c3df9", "上网行为管理-专业版"),
            ("9979ba6130cf4ba4a004a117d62c2634", "逍遥旅游"),
            ("9e634e2414514832887a56a62d29b135", "（通算）内存优化型云主机"),
            ("e99a08ca34e3448aa679a8ede6aed374", "（通算）GPU加速型云主机"),
            ("5cffa6f7565f4633be80b261988f7dba", "用户增长引擎-应用"),
            ("12407f55b634487eae00504d750b486a", "数据安全中心"),
            ("0eca5a707c67439fb54b7b17e34a5aff", "安全资源池行业版"),
            ("4a27503db32b4b1993fd79bd79846483", "边缘云端口"),
            ("e64e8bb9313a48f092f1d7c6cbe35e4f", "态势感知-混合云版"),
            ("1eb757e677d7472189921604f4ae5ae2", "云安全中心-终端安全-混合云版"),
            ("ddd10373cdee4056866388390906faa8", "云堡垒机-混合云版"),
            ("164f486659234469a3ce3593a1994347", "云身份安全服务-混合云版"),
            ("d9294e900c9a49a9a266908aba572c20", "通算对象存储"),
            ("8bbb341b16a4455eb011ae7c90197c99", "安全资源池专业版"),
            ("077d55ecbac64eae88a29901ecc77161", "5G云梯"),
            ("b689ad891ff245328182911ff7aff047", "边缘GPU专享裸金属"),
            ("1d6c4f542ea64a8ca26d7bf01ba7a3dc", "多模态内容理解"),
            ("c91c848c540a41a3b29c5c2fb3ba9f5f", "场景化加速"),
            ("38f0c0e0f25917f61b6dddc05de4de33", "云空间"),
            ("8c5d1961a5ab4c11b72e35d0d86d1081", "（河南）裸金属服务器"),
            ("d024c83bff78402b896e3a4a673046b9", "智能计算型裸金属"),
            ("a1bf1a99986340c88102eedb8102ef6f", "（智算）高性能存储"),
            ("d8c42e3c23c44dbeaa0932e4f80d5d8e", "云数据库防火墙-专业版"),
            ("d36f51334cdc41ab81cd3427de4d9d3a", "Web全栈防护-混合云版"),
            ("e06a51fc0f6a44e892f0127349145b5f", "日志审计混合云-日志版"),
            ("3165976e2a0245acb137bda3813ed66b", "日志审计混合云-流量版"),
            ("a1cfca0946894e2ba6823200d9eed202", "增强漏洞扫描-配置核查版"),
            ("163482cf9c9144f6939a8cb6f0f1ce04", "近源抗D-流量监控"),
            ("407079aad1a74e799372586910176235", "近源抗D-流量清洗"),
            ("e0dac1bb55a44e22a22ba8b7f57861e2", "近源抗D-保障服务"),
            ("fbf5a571bd9d4542bbe3f23d53485d1c", "近源抗D-攻防演练服务"),
            ("1b99dd8fd1ea4b62b1dc511e8762bd09", "云下一代防火墙-混合云版 "),
            ("0e31a7554a7940a5bcf3fb74cb2f65bb", "数据库防火墙-混合云版 "),
            ("f6ed2b72448b4a6cb7dc904397321d53", "安全隔离信息交换系统-混合云版"),
            ("adfb8bf096124261825063dc527ffe39", "入侵防御-混合云版"),
            ("e1dee35b38d44084ad2d32f83be4d38a", "入侵检测-混合云版"),
            ("9710c06b000345c190e72f7266b9cb29", "增强漏洞扫描混合云-综合漏扫版"),
            ("9b40f70b5365426993941a86b6aa0097", "移动云达梦数据库"),
            ("b535e038307f43c59f6ad99255298f35", "网站安全卫士-网站监测-混合云版"),
            ("5231417ef22b4a929e657511992f0fd1", "GPU型云主机（许昌）"),
            ("c42252ab357548d0b377ddab658a6206", "云安全中心-欺骗防御系统"),
            ("1350090f535c4a83a2cca4898f85c9cd", "云原生应用安全-混合云版"),
            ("f599ccf30cb942f0933b224dbbcf1d83", "高防抗D"),
            ("a176265378124087b939a3daf5c38fde", "近源抗D-流量压制"),
            ("66105b2a5e984a729bee0413dcf290bd", "安全服务"),
            ("108503d768274a92b3203b6bbd105b2f", "云信"),
            ("da928106fe364875a38dc7469def201f", "SSL证书"),
            ("e062211d1dcf461992bd553e98adda50", "数据快递"),
        ].iter().map(|(k,v)| (k.to_string(), v.to_string())).collect();

        let  date1 = &self.end_date[0..4].parse::<i32>()?;
        let sql= format!("select USER_ID,CUST_ID,CUST_NAME from tf_b_trade where user_id in ({0}) union all
        select USER_ID,CUST_ID,CUST_NAME from tf_b_trade_{1} where user_id in ({0}) union all
        select USER_ID,CUST_ID,CUST_NAME from tf_b_trade_{2} where user_id in ({0}) union all
        select USER_ID,CUST_ID,CUST_NAME from tf_b_trade_{3} where user_id in ({0});",user_ids,date1,date1-1,date1-2);
        let app=Apps::new(&self.jour_url,"jour");
        let data = app.get_data(&sql).await?;
        let result: HashMap<String, String> = data.iter().filter_map(|v| {
            let user_id = v.get("USER_ID")?.as_str()?.to_string();
            let cust_name = v.get("CUST_NAME")?.as_str()?.to_string();
            Some((user_id, cust_name))
        }).collect();
        let result1 = result1.into_iter()
            .map(|mut x| {
                let custs = match x.get("USER_ID") {
                    Some(Value::String(id)) => result.get(id).map(|s| s.as_str()).unwrap_or(""),
                    _ => "",
                };
                let longs = match x.get("PRODUCT_ID") {
                    Some(Value::String(id)) => long.get(id).map(|s| s.as_str()).unwrap_or(""),
                    _ => "",
                };
                x.insert("cust_name".to_string(), json!(custs));

                x.insert("if_long".to_string(), json!(longs));
                x
            })
            .collect();
        Ok(result1)
    }

    async fn get_anser(&self,user_ids:String,result1:Vec<HashMap<String, Value>>)-> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>>{
        let sql= format!("select  a.user_id_b as USER_ID_B,a.user_id_a,a.RELATION_TYPE_CODE,b.user_state,max(c.end_date) as end_date
from tf_f_relation_uu a
inner join tf_f_user b
inner join tf_f_user_product c
on a.user_id_a=b.user_id and a.user_id_a=c.user_id
where a.user_id_b in (
{}
) and a.RELATION_TYPE_CODE in ('91','94') group by a.user_id_b;",user_ids);
        let app=Apps::new(&self.juhe_url,"crm");
        let data = app.get_data(&sql).await?;
        let result:HashMap<String,(String,String)> = data.iter().filter_map(|v| {
            let user_id = v.get("USER_ID_B")?.as_str()?.to_string();
            let status = v.get("USER_STATE")?.as_str()?.to_string();
            let end_dates  = v.get("END_DATE")?.as_str()?.to_string();
            Some((user_id, (status, end_dates)))
        }).collect();
        let result1 = result1.into_iter()
            .map(|mut x| {
                let status = match x.get("USER_ID") {
                    Some(Value::String(id)) => result.get(id).map(|s| s.0.as_str()).unwrap_or(""),
                    _ => "",
                };
                let end_dates = match x.get("USER_ID") {
                    Some(Value::String(id)) => result.get(id).map(|s| s.1.as_str()).unwrap_or(""),
                    _ => "",
                };
                x.insert("order_status".to_string(), json!(status));

                x.insert("order_date".to_string(), json!(end_dates));
                x
            })
            .collect();
        Ok(result1)
    }

    pub async fn get_data(&mut self) -> Result<(HashMap<String,String>,Vec<HashMap<String,Value>>), Box<dyn std::error::Error>> {
        let num_all = self.get_num().await?;
        let res1 = self.get_renew().await?;
        let res2 = self.get_destroy().await?;

        let user_id_str1:String=res1.iter().map(|map| {
            map.get("USER_ID").unwrap().as_str().unwrap().to_string()
        } ).collect::<Vec<String>>()
            .join(",");
        self.user_id_str1=user_id_str1;
        let result11 = self.if_renew(self.user_id_str1.clone(), res1).await?;
        let result12 = self.get_version(self.user_id_str1.clone(), "INSTANCE_EXPIRE_AUTO_RENEW",result11).await?;
        let result13 = self.if_ceshi(self.user_id_str1.clone(), result12).await?;
        let result14 = self.get_anser(self.user_id_str1.clone(), result13).await?;
        let mut num = 0;
        let mut  renew_num = Vec::new();
        for dd in result14.into_iter() {
            if dd.get("aee_err").unwrap_or(&Value::Null).as_str().unwrap_or("").contains("当前账号状态异常"){
                num+=1
            }else {
                renew_num.push(dd);
            }
        }

        let user_id_str2:String=res2.iter().map(|map| {
            map.get("USER_ID").unwrap().as_str().unwrap().to_string()
        } ).collect::<Vec<String>>()
            .join(",");
        self.user_id_str2=user_id_str2;
        let result21 = self.if_renew(self.user_id_str2.clone(), res2).await?;
        let result22 = self.get_version(self.user_id_str2.clone(), "INSTANCE_EXPIRE_AUTO_DESTROY",result21).await?;
        let result23 = self.if_ceshi(self.user_id_str2.clone(), result22).await?;
        let result24 = self.get_anser(self.user_id_str2.clone(), result23).await?;
        let mut num2 = 0;
        let mut  destroy_num = Vec::new();
        for dd in result24.into_iter() {
            if dd.get("aee_err").unwrap_or(&Value::Null).as_str().unwrap_or("").contains("暂停天数未满【30】天"){
                num2+=1
            }else {
                destroy_num.push(dd);
            }
        }
        renew_num.append(&mut destroy_num);
        Ok((num_all,renew_num))

    }


}



