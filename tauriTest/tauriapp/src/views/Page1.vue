<template>
  <el-text style="display: inline-block; margin: 10px 0; margin-left: 10px;" type="info" >到期业务异常巡检页面</el-text>
  <div>
    <el-form :model="Formauto" label-width="120px" :rules="formRulesauto">
      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="crm聚合库地址" prop="juhe1">
            <el-input  v-model="Formauto.juhe1" placeholder="请输入聚合库地址：http://"  />
          </el-form-item>
        </el-col>
      </el-row>

      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="jour订单库地址" prop="jour1">
            <el-input v-model="Formauto.jour1" placeholder="请输入订单库库地址：http://" />
          </el-form-item>
        </el-col>
      </el-row>
      <el-row :gutter="20">
        <el-col :span="15">
          <el-form-item label="日期范围" prop="value1" >
            <el-date-picker
                v-model="Formauto.value1"
                type="daterange"
                range-separator="To"
                start-placeholder="开始日期"
                end-placeholder="结束日期"
                :size="size"
                value-format="YYYY-MM-DD"
            />
          </el-form-item>
        </el-col>
        <el-col :span="9" >
          <el-button type="primary" class="kuang" style="margin-left: 10px;" @click="submitauto"
           :loading="loading"
           :disabled="loading"
          >到期业务巡检</el-button>
          <el-button type="primary" class="kuang" style="margin-left: 10px;" @click="exportTableToCSV">导出为CSV</el-button>
        </el-col>
      </el-row>
    </el-form>
    <div style="margin-top: 20px;">
      <el-row class="w-150px mb-2">
        <el-text truncated>{{numvalue}}</el-text>
      </el-row>
    </div>
    <div v-loading="loading" element-loading-text="生成中，请稍等...">
      <el-table-v2
          :columns="columns"
          :data="tableData"
          :width="720"
          :height="440"
          fixed
          style="overflow: hidden;"
      />
    </div>

      <el-row class="w-150px mb-2">
        <el-text truncated>温馨提示：</el-text>
      </el-row>
      <el-text line-clamp="2">
        1、由于到期巡检sql为慢查询sql，执行结束大约需要二十分钟，请耐心等待！<br />
        2、针对到期业务的巡检，聚合库需要打开crm界面<br />
      </el-text>

  </div>
</template>


<script setup lang="ts">
import { reactive,ref } from "vue";
import { writeTextFile,BaseDirectory } from '@tauri-apps/plugin-fs';
import { invoke } from "@tauri-apps/api/core";

const size = ref('default')
const Formauto = reactive({juhe1: "", jour1: "", value1:[] as string[]});
let tableData = ref<AutoInfo[]>([]);

let numvalue = ref("");
const loading = ref(false);


// 表单验证规则
const formRulesauto = reactive({
  juhe1: [
    { required: true, message: '请输入聚合库网页地址', trigger: 'blur' }
  ],
  jour1: [
    { required: true, message: '请输入订单库网页地址', trigger: 'blur' }
  ],
  value1: [
    { required: true, message: '请输入日期范围', trigger: 'blur' }
  ],
});

interface AutoInfo {
  CUST_ID: string;
  cust_name: string;
  USER_ID: string;
  PRODUCT_ID: string;
  REMOVE_TAG: string;
  USER_STATE: string;
  END_DATE: string;
  aee_err: string;
  if_renew: string;
  if_long: string;
  order_status: string;
  order_date: string;
  type1?:string;
}
const headerLabelMap: Record<string, string> = {
  "到期类型":"type1",
  "客户id":"CUST_ID",
  "客户名":"cust_name",
  "USER_ID": "USER_ID",
  "产品id":"PRODUCT_ID",
  "REMOVE_TAG": "REMOVE_TAG",
  "USER_STATE": "USER_STATE",
  "到期时间": "END_DATE",
  "报错信息":"aee_err",
  "是否自动续订": "if_renew",
  "长流程信息": "if_long",
  "主订单状态": "order_status",
  "主订单到期时间":"order_date"
};
const columns = Object.keys(headerLabelMap).map(key => ({
  key: headerLabelMap[key],
  dataKey: headerLabelMap[key],
  title: `${key}`,
  width: 120
}));
const generateData = (data:AutoInfo[] ) =>
    data.map((item ,index)=> ({
      ...item,
      id: `row-${index}`,
      parentId: null,
    })) as (AutoInfo & { id: string ,parentId:null})[];
const submitauto = async () => {
  try {
    loading.value = true
    const [start, end] = Formauto.value1;
    const data = {jour2:Formauto.jour1,
      juhe2: Formauto.juhe1,
      startdate: start,
      enddate: end,}
    console.log(data)
    let [numAll, autoAll]  = await invoke<[Record<string, string>,AutoInfo[]]>("libauto", { jsonStr: JSON.stringify(data) });
    const currentDate = new Date();
    const threeDaysAgo = new Date(currentDate);
    threeDaysAgo.setDate(currentDate.getDate() - 10);
    autoAll.forEach(auto => {
      const inputDate = new Date(auto.END_DATE);
      if (inputDate<threeDaysAgo) {
        auto.type1= "到期退订"
      }else {
        if (auto.if_renew==="开启") {
          auto.type1= "自动续订"
        }else {
          auto.type1= "到期暂停"
        }
      }
    })
    tableData.value = generateData(autoAll);

    console.log(tableData);
    let str = "此段时间应发起到期任务数据：";
    for (const dd in numAll) {
      if (numAll.hasOwnProperty(dd)) {
        str += `${dd}:${numAll[dd]}  `;
      }
    }
    numvalue.value = str;
  } catch (error) {
    console.error("巡检出错:", error);
  } finally {loading.value = false}
}
//转译csv字段
function escapeCSV(value: string): string {
  if (value == null) return '';
  const needsEscape = /[",\n]/.test(value);
  const escaped = value.replace(/"/g, '""'); // 转义双引号
  return needsEscape ? `"${escaped}"` : escaped;
}
const exportTableToCSV = async () => {

  const headers = ["到期类型","客户id","客户名","USER_ID","产品id","REMOVE_TAG","USER_STATE","到期时间","报错信息","是否自动续订","长流程信息","主订单状态","主订单到期时间"];

  const rows = tableData.value.map(row => {
    return headers.map(header => escapeCSV(String(row[headerLabelMap[header] as keyof AutoInfo]))).join(',');
  });
  // 构建带 BOM 的 CSV 内容（确保 Excel 支持中文）
  const bom = '\uFEFF';
  const csvContent = bom + [headers.join(','), ...rows].join('\n');

  const now = new Date();
  const textpath =`到期业务巡检${now.getFullYear().toString()}-${now.getMonth()+1}-${now.getDate()}.csv`;
  // 写入到 Downloads 文件夹
  await writeTextFile(textpath, csvContent, {
    baseDir: BaseDirectory.Download
  });

  alert(`文件已保存到你的下载目录，请注意查看`);
}


</script>

<style scoped>
.kuang{
  margin-left: 40px;
}
</style>