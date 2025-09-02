<template>
  <el-text style="display: inline-block; margin: 10px 0; margin-left: 25px;"  type="info">同步清分失败修复页面</el-text>
  <div>
    <el-form :model="Formqingfen" label-width="120px" :rules="formRulesqingfen">
      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="订单库地址" prop="jour1">
            <el-input  v-model="Formqingfen.jour1" placeholder="请输入jour库地址：http://"  />
          </el-form-item>
        </el-col>
      </el-row>

      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="输入订单号" prop="trades">
            <el-input v-model="Formqingfen.trades" placeholder="请输入trade_id,批量数据用空格隔开" />
          </el-form-item>
        </el-col>
      </el-row>
      <el-row :gutter="20">
        <el-col :span="14">
          <img src="../assets/222.png" alt="填充图片" title="填充图片" style="margin-left: 30px;" width="440" height="60">
        </el-col>
        <el-col :span="10">
          <el-button type="primary"  class="kuang" @click="submitqingfen" :loading="loading" :disabled="loading">生成执行sql</el-button>
          <el-button type="primary"  class="kuang" @click="exportTableToCSV">导出为CSV</el-button>
        </el-col>
      </el-row>
    </el-form>
    <div v-loading="loading" element-loading-text="生成中，请稍等...">
      <el-table-v2
          :columns="columns"
          :data="tableData"
          :width="720"
          :height="440"
          fixed
          class="biao"
      />
    </div>
      <el-row class="w-150px mb-2">
        <el-text truncated>温馨提示：</el-text>
      </el-row>
      <el-text line-clamp="3">
        1、该功能旨在处理订单库同步清分失败的订单<br />
        2、根据生成的inser语句顺序执行，触发手动同步清分操作<br />
      </el-text>

  </div>
</template>


<script setup lang="ts">
import { reactive,ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { writeTextFile,BaseDirectory } from '@tauri-apps/plugin-fs';

const Formqingfen = reactive({jour1: "", trades: ""});
const loading = ref(false);
let tableData = ref<Qing[]>([]);

const headerLabelMap: Record<string, string> = {
  "i_user":"user_sql",
  "i_account":"account_sql",
  "i_discnt":"discnt_sql",
  "i_product":"product_sql",
  "prod_char_value":"attr_sql",
  "i_resource":"res_sql",
  "i_user_mapping":"mapping_sql",
  "i_user_relation":"uu_sql",
  "i_user_status":"status_sql",
  "i_data_index":"index_sql",
}

interface Qing {
  user_sql?:String,
  account_sql?:String,
  discnt_sql? :String,
  product_sql?:String,
  attr_sql?:String,
  res_sql?:String,
  mapping_sql?:String,
  uu_sql?:String,
  status_sql?:String,
  index_sql:String,
}
// 表单验证规则
const formRulesqingfen = reactive({

  jour1: [
    { required: true, message: '请输入订单库网页地址', trigger: 'blur' }
  ],
  trades: [
    { required: true, message: '请输入修复的订单号', trigger: 'blur' }
  ],

});

const columns = Object.keys(headerLabelMap).map(key => ({
  key: headerLabelMap[key],
  dataKey: headerLabelMap[key],
  title: `${key}表`,
  width: 140
}));
const generateData = (data:Qing[] ) =>
    data.map((item ,index)=> ({
      ...item,
      id: `row-${index}`,
      parentId: null,
    })) as (Qing & { id: string ,parentId:null})[];

const submitqingfen = async () => {
  try {
    loading.value = true
    console.log(Formqingfen);
    let res: Qing[]  = await invoke("libqingfen", { jsonStr: JSON.stringify(Formqingfen) });
    console.log(res);

    tableData.value = generateData(res);
  } catch (error) {
    console.error("获取清分数据出错:", error);
  }finally {loading.value = false}}
//转译csv字段
function escapeCSV(value: string): string {
  if (value == null) return '';
  const needsEscape = /[",\n]/.test(value);
  const escaped = value.replace(/"/g, '""'); // 转义双引号
  return needsEscape ? `"${escaped}"` : escaped;
}
const exportTableToCSV = async () => {

  const headers =["i_user", "i_account","i_discnt","i_product","prod_char_value","i_resource","i_user_mapping","i_user_relation","i_user_status","i_data_index"];

  // 转换每一行数据
  const rows = tableData.value.map(row => {
    return headers.map(header => escapeCSV(String(row[headerLabelMap[header] as keyof Qing]))).join(',');
  });


  // 构建带 BOM 的 CSV 内容（确保 Excel 支持中文）
  const bom = '\uFEFF';
  const csvContent = bom + [headers.join(','), ...rows].join('\n');

  const now = new Date();
  const textpath =`同步清分执行sql${now.getFullYear().toString()}-${now.getMonth()+1}-${now.getDate()}.csv`;
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

.biao {
  margin-top: 20px;
  overflow: hidden;
}
</style>