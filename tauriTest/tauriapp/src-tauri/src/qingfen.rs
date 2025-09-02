use std::collections::HashMap;
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, CONNECTION, ORIGIN, REFERER, USER_AGENT};
use serde_json::{json, Value};


pub struct Apps {
    url: String,
    ku: String,
    header:HeaderMap<HeaderValue>,
    url0:String,
    url1: String,
    url2: String,
    trades: String,
}
impl Apps {
    pub fn new(url: &str,tradesss:&str) -> Self {
        let tradess = tradesss.split(" ").map(|s| s.to_string()).collect::<Vec<String>>().join("','");
        Apps {
            url: url.to_string(),
            ku: "jour".to_string(),
            header:HeaderMap::new(),
            url0:String::new(),
            url1: String::new(),
            url2: String::new(),
            trades: tradess,
        }
    }
    fn extract_host_id(&self) -> Result<(String, String),Box<dyn std::error::Error>> {
        // 匹配主机部分（非贪婪匹配直到 /#）
        let host_re = Regex::new(r"http://(.*?)/#")?;
        let host = host_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| "URL 格式错误，无法提取主机")?;

        // 匹配 ID 部分
        let id_re = Regex::new(r"id=([^&]*)")?;
        let id = id_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| "URL 格式错误，无法提取 ID")?;

        Ok((host, id))
    }

    pub fn head(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        let (host, id) = self.extract_host_id()?;
        self.url0 = format!("http://{}/WebDb/sql/parse", host);
        self.url1 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=true", host, id, self.ku);
        self.url2 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=&length=1000", host, id, self.ku);

        // 构建请求头
        self.header.clear();
        self.header.insert(ACCEPT, HeaderValue::from_static("application/json, text/plain, */*"));
        self.header.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        self.header.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9,ja;q=0.8"));
        self.header.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        self.header.insert(ORIGIN, HeaderValue::from_str(&format!("http://{}", host))?);
        self.header.insert(REFERER, HeaderValue::from_str(&format!("http://{}/", host))?);
        self.header.insert(
            USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
        );

        Ok(self)
    }

    async fn get_data(&self, sql: &str) -> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        // 第一个请求 (url0)
        let data0 = json!({"sql": sql.replace("\n"," "), "type": "sql"});
        let _res0 = client
            .post(&self.url0)
            .headers(self.header.clone())
            .json(&data0)
            .send()
            .await?;

        // 第二个请求 (url1)
        let data = json!({"sql": sql.replace("\n"," ")});
        let _res1 = client
            .post(&self.url1)
            .headers(self.header.clone())
            .json(&data)
            .send()
            .await?;

        // 第三个请求 (url2)
        let res2 = client
            .post(&self.url2)
            .headers(self.header.clone())
            .json(&data)
            .send()
            .await?;

        // 解析响应
        let response_text = res2.text().await?;

        let json_response: Value = serde_json::from_str(&response_text)?;

        let result = json_response.get("data")
            .and_then(|v| v.as_array())
            .and_then(|v|v.get(0))
            .and_then(|v|v.get("data"))
            .and_then(|v|v.get("data"))
            .and_then(|v| v.as_array())
            .unwrap_or(&vec![])
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

    async fn to_hashmap(&self,sql:&str) -> Result<HashMap<String, HashMap<String,Value>>,Box<dyn std::error::Error>> {
        let data=&self.get_data(&sql).await?;
        let result: HashMap<String, HashMap<String,Value>> = data.iter().filter_map(|v| {
            let TRADE_ID = v.get("TRADE_ID")?.as_str()?.to_string();
            Some((TRADE_ID, v.clone()))
        }).collect();
        Ok(result)

    }

    fn to_index(&self,data:&HashMap<String, Value>,tag:String) -> Result<String,Box<dyn std::error::Error>> {
        let CUST_ID=data.get("CUST_LONG_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let ACCT_ID=data.get("ACCT_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let USER_ID=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let UP_FIELD=tag;
        let REGION_CODE:i64=data.get("EPARCHY_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.parse::<i64>().unwrap())
            .unwrap_or_default();
        let COUNTY_CODE:i64=data.get("EPARCHY_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.parse::<i64>().unwrap())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let SO_NBR=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let REMARK=data.get("REMARK")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let DONE_CODE=data.get("TRADE_TYPE_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let BUSI_CODE=data.get("SYNC_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')",CUST_ID,ACCT_ID,USER_ID,UP_FIELD,REGION_CODE,COUNTY_CODE,commitDate,SO_NBR,REMARK,DONE_CODE,BUSI_CODE);
        Ok(info)
    }

    fn get_user(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let INST_ID="0".to_string();
        let USER_ID=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let CUST_ID=data.get("CUST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let ACCT_ID=data.get("CUST_LONG_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let LOGIN_ID= data.get("LOGIN_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let province=data.get("PROVINCE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userState=data.get("USER_STATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let prepayTag=data.get("PREPAY_TAG")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let removeTag=data.get("REMOVE_TAG")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userTagSet=data.get("USER_TAG_SET")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate=data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate=data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let innerCode=data.get("INNER_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')",
                           INST_ID,USER_ID,CUST_ID,ACCT_ID,LOGIN_ID,province,userState,prepayTag,removeTag,userTagSet,validDate,expireDate,operType,soNbr,commitDate,innerCode);
        Ok(info)
    }

    fn get_account(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let acctId=data.get("ACCT_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let custId=data.get("CUST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let acctStatus=data.get("ACCT_STATUS")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let acctName=data.get("ACCT_NAME")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let loginName= data.get("LOGIN_NAME")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate=data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate=data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')",
                           acctId,custId,acctStatus,acctName,loginName,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_discnt(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let instId=data.get("INST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let discntCode=data.get("DISCNT_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let productId=data.get("PRODUCT_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate= data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate=data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}')",
                           instId,discntCode,userId,productId,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_product(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let instId=data.get("INST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let productId=data.get("PRODUCT_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let mainTag=data.get("MAIN_TAG")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate= data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate=data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}')",
                           instId,userId,productId,mainTag,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_attr(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let instId=data.get("INST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let instType=data.get("INST_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let relInstId=data.get("REL_INST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let attrCode= data.get("ATTR_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let attrValue=data.get("ATTR_VALUE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate=data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate=data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')",
                           instId,userId,instType,relInstId,attrCode,attrValue,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_res(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let instId=data.get("INST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let resCode=data.get("RES_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate=data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate= data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();


        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}')",
                           instId,resCode,userId,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_mapping(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let instId=data.get("INST_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let mappingCode=data.get("MAPPING_CODE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let mappingValue=data.get("MAPPING_VALUE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate= data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate=data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();


        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}')",
                           instId,userId,mappingCode,mappingValue,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_uu(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let relationType=data.get("RELATION_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let oppUserId=data.get("OPP_USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate=data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate= data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();



        let info = format!("('{}','{}','{}','{}','{}','{}','{}','{}')",
                           userId,relationType,oppUserId,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }

    fn get_status(&self,data:&HashMap<String, Value>) -> Result<String,Box<dyn std::error::Error>> {
        let userId=data.get("USER_ID")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let userState=data.get("USER_STATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let validDate=data.get("VALID_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let expireDate= data.get("EXPIRE_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let operType=data.get("OPER_TYPE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let soNbr=data.get("SO_NBR")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let commitDate=data.get("COMMIT_DATE")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();



        let info = format!("('{}','{}','{}','{}','{}','{}','{}')",
                           userId,userState,validDate,expireDate,operType,soNbr,commitDate);
        Ok(info)
    }


    pub async fn get_index_info(&self)-> Result<Vec<HashMap<String,String>>,Box<dyn std::error::Error>> {
        let sql_sync= format!("select * from ti_b_synchinfo where trade_id in ('{0}');",&self.trades);
        let sql_user= format!("select * from TI_B_USER where trade_id in ('{0}');",&self.trades);
        let sql_account= format!("select * from TI_B_ACCOUNT where trade_id in ('{0}');",&self.trades);
        let sql_discnt= format!("select * from TI_B_USER_DISCNT where trade_id in ('{0}');",&self.trades);
        let sql_product= format!("select * from TI_B_USER_PRODUCT where trade_id in ('{0}');",&self.trades);
        let sql_attr= format!("select * from TI_B_USER_ATTR where trade_id in ('{0}');",&self.trades);
        let sql_res= format!("select * from TI_B_USER_RES where trade_id in ('{0}');",&self.trades);
        let sql_ampping= format!("select * from TI_B_USER_MAPPING where trade_id in ('{0}');",&self.trades);
        let sql_uu= format!("select * from TI_B_RELATION_UU where trade_id in ('{0}');",&self.trades);
        let sql_status= format!("select * from TI_B_USER_STATUS where trade_id in ('{0}');",&self.trades);

        let data_sync = &self.to_hashmap(&sql_sync).await?;
        let data_user = &self.to_hashmap(&sql_user).await?;
        let data_account = &self.to_hashmap(&sql_account).await?;
        let data_discnt = &self.to_hashmap(&sql_discnt).await?;
        let data_product = &self.to_hashmap(&sql_product).await?;
        let data_attr= &self.to_hashmap(&sql_attr).await?;
        let data_res = &self.to_hashmap(&sql_res).await?;
        let data_mapping = &self.to_hashmap(&sql_ampping).await?;
        let data_uu = &self.to_hashmap(&sql_uu).await?;
        let data_status = &self.to_hashmap(&sql_status).await?;

        let mut vec_index: Vec<HashMap<String, String>> = Vec::from([
            HashMap::from([
                ("user_sql".to_string(), "insert into jd.i_user (INST_ID,USER_ID,CUST_ID,ACCT_ID,LOGIN_ID,PROVINCE,USER_STATE,PREPAY_TAG,REMOVE_TAG,USER_TAG_SET,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE,INNER_CODE ) values".to_string()),
                ("account_sql".to_string(), "insert into jd.i_account (ACCT_ID,CUST_ID,ACCT_STATUS,ACCT_NAME,LOGIN_NAME,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("discnt_sql".to_string(), "insert into jd.i_discnt (INST_ID,DISCNT_CODE,USER_ID,PRODUCT_ID,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("product_sql".to_string(), "insert into jd.i_product (INST_ID,USER_ID,PRODUCT_ID,MAIN_TAG,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("attr_sql".to_string(), "insert into jd.i_prod_char_value (INST_ID,USER_ID,INST_TYPE,REL_INST_ID,ATTR_CODE,ATTR_VALUE,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("res_sql".to_string(), "insert into jd.i_resource (INST_ID,RES_CODE,USER_ID,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("mapping_sql".to_string(), "insert into jd.i_user_mapping (INST_ID,USER_ID,MAPPING_CODE,MAPPING_VALUE,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("uu_sql".to_string(), "insert into jd.i_user_mapping (INST_ID,USER_ID,MAPPING_CODE,MAPPING_VALUE,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("status_sql".to_string(), "insert into jd.i_user_status (USER_ID,RELATION_TYPE,OPP_USER_ID,VALID_DATE,EXPIRE_DATE,OPER_TYPE,SO_NBR,COMMIT_DATE ) values".to_string()),
                ("index_sql".to_string(), "insert into jd.i_data_index (CUST_ID,ACCT_ID,USER_ID,UP_FIELD,REGION_CODE,COUNTY_CODE,COMMIT_DATE,SO_NBR,REMARK,DONE_CODE,BUSI_CODE ) values".to_string()),
            ])
        ]);

        for (k1,v1) in data_sync.iter() {
            let table = v1
                .get("SYNC_TAB_NAME")
                .ok_or("SYNC_TAB_NAME 字段缺失")?
                .as_str()
                .ok_or("SYNC_TAB_NAME 字段缺失")?
                .split(",").collect::<Vec<&str>>();
            let mut tag = String::from("00000000000000000000000000000000");
            let mut hasmap:HashMap<String,String> = HashMap::new();
            for k2 in table.iter() {
                match *k2 {
                    "TI_B_USER" => {
                        println!("k2{}",k2);
                        let trade_info = data_user.get(k1).unwrap();
                        let datas = self.get_user(trade_info)?;
                        hasmap.insert("user_sql".to_string(),format!("{},",datas));
                        tag.replace_range(3..4, "1");

                    },
                    "TI_B_ACCOUNT" => {
                        let trade_info = data_account.get(k1).unwrap();
                        let datas = self.get_account(trade_info)?;
                        hasmap.insert("account_sql".to_string(),format!("{},",datas));
                        tag.replace_range(1..2, "1");
                    },
                    "TI_B_USER_DISCNT" => {
                        let trade_info = data_discnt.get(k1).unwrap();
                        let datas = self.get_discnt(trade_info)?;
                        hasmap.insert("discnt_sql".to_string(),format!("{},",datas));
                        tag.replace_range(10..11, "1")
                    },
                    "TI_B_USER_PRODUCT" => {
                        let trade_info = data_product.get(k1).unwrap();
                        let datas = self.get_product(trade_info)?;
                        hasmap.insert("product_sql".to_string(),format!("{},",datas));
                        tag.replace_range(4..5, "1")
                    },
                    "TI_B_USER_ATTR" => {
                        let trade_info = data_attr.get(k1).unwrap();
                        let datas = self.get_attr(trade_info)?;
                        hasmap.insert("attr_sql".to_string(),format!("{},",datas));
                        tag.replace_range(5..6, "1")
                    },
                    "TI_B_USER_RES" => {
                        let trade_info = data_res.get(k1).unwrap();
                        let datas = self.get_res(trade_info)?;
                        hasmap.insert("res_sql".to_string(),format!("{},",datas));
                        tag.replace_range(8..9, "1")
                    },
                    "TI_B_USER_MAPPING" => {
                        let trade_info = data_mapping.get(k1).unwrap();
                        let datas = self.get_mapping(trade_info)?;
                        hasmap.insert("mapping_sql".to_string(),format!("{},",datas));
                        tag.replace_range(6..7, "1")
                    },
                    "TI_B_RELATION_UU" => {
                        let trade_info = data_uu.get(k1).unwrap();
                        let datas = self.get_uu(trade_info)?;
                        hasmap.insert("uu_sql".to_string(),format!("{},",datas));
                        tag.replace_range(7..8, "1")
                    },
                    "TI_B_USER_STATUS" => {
                        let trade_info = data_status.get(k1).unwrap();
                        let datas = self.get_status(trade_info)?;
                        hasmap.insert("status_sql".to_string(),format!("{},",datas));
                        tag.replace_range(9..10, "1")
                    },
                    _ => { println!("")}
                }

            }
            let sql_index= self.to_index(v1,tag).unwrap();
            hasmap.insert("index_sql".to_string(),format!("{},",sql_index));
            vec_index.push(hasmap);

        };



        Ok(vec_index)


    }

}

