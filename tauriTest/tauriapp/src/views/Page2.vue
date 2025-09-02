<template>
  <el-text style="display: inline-block; margin: 10px 0; margin-left: 25px;"  type="info">产品基础口令异常未触发修复页面</el-text>
  <div>
    <el-form :model="Formjichu" label-width="120px" :rules="formRulesjichu">
      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="聚合库地址" prop="juhe1">
            <el-input  v-model="Formjichu.juhe1" placeholder="请输入聚合库地址：http://"  />
          </el-form-item>
        </el-col>
      </el-row>

      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="输入user_id" prop="user_id">
            <el-input v-model="Formjichu.user_id" placeholder="请输入user_id,批量数据用空格隔开" />
          </el-form-item>
        </el-col>
      </el-row>
      <el-row :gutter="10">
        <el-col :span="15">
          <el-radio-group v-model="Formjichu.type1" class="kuang">
            <el-radio :value="1">基础口令恢复</el-radio>
            <el-radio :value="2">基础口令暂停</el-radio>
            <el-radio :value="3">基础口令退订</el-radio>
          </el-radio-group>
        </el-col>
        <el-col :span="9">
          <el-button type="primary"  class="kuang" @click="submitjichu">生成执行sql</el-button>
          <el-button type="primary"   @click="exportTableToCSV">导出为CSV</el-button>
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
      1、该功能旨在处理产品级订单没有触发基础口令暂停、基础口令恢复或基础口令退订的问题<br />
      2、聚合库需要打开crm界面，根据生成的inser语句顺序执行，触发基础口令操作<br />
    </el-text>
  </div>
</template>


<script setup lang="ts">
import { reactive,ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { writeTextFile,BaseDirectory } from '@tauri-apps/plugin-fs';

const Formjichu = reactive({juhe1: "", user_id: "", type1:1});
const loading = ref(false);
let tableData = ref<DealBat[]>([]);
// 表单验证规则
const formRulesjichu = reactive({

  juhe1: [
    { required: true, message: '请输入聚合库网页地址', trigger: 'blur' }
  ],
  user_id: [
    { required: true, message: '请输入user_id', trigger: 'blur' }
  ],

});
interface DealBat {
  type2:String,
  bat1:String,
  bat2:String,
  check1:String,
}
const headerLabelMap: Record<string, string> = {
  "操作类型":"type2",
  "执行sql1":"bat1",
  "执行sql2":"bat2",
  "检查sql":"check1",

}
const columns = Object.keys(headerLabelMap).map(key => ({
  key: headerLabelMap[key],
  dataKey: headerLabelMap[key],
  title: `${key}`,
  width: 180
}));
const generateData = (data:DealBat[] ) =>
    data.map((item ,index)=> ({
      ...item,
      id: `row-${index}`,
      parentId: null,
    })) as (DealBat & { id: string ,parentId:null})[];

const submitjichu = async () => {
  try {
    loading.value = true
    console.log(Formjichu);
    let res: DealBat[]  = await invoke("jichu1", { jsonStr: JSON.stringify(Formjichu) });
    console.log(res);
    tableData.value = generateData(res);
  } catch (error) {
    console.error("基础口令出错:", error);
  }finally {loading.value = false}}
//转译csv字段
function escapeCSV(value: string): string {
  if (value == null) return '';
  const needsEscape = /[",\n]/.test(value);
  const escaped = value.replace(/"/g, '""'); // 转义双引号
  return needsEscape ? `"${escaped}"` : escaped;
}
const exportTableToCSV = async () => {

  const headers =["操作类型", "执行sql1","执行sql2","检查sql"];

  // 转换每一行数据
  const rows = tableData.value.map(row => {
    return headers.map(header => escapeCSV(String(row[headerLabelMap[header] as keyof DealBat]))).join(',');
  });


  // 构建带 BOM 的 CSV 内容（确保 Excel 支持中文）
  const bom = '\uFEFF';
  const csvContent = bom + [headers.join(','), ...rows].join('\n');

  const now = new Date();
  const textpath =`基础口令操作${now.getFullYear().toString()}-${now.getMonth()+1}-${now.getDate()}.csv`;
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
  margin-top: 40px;
  overflow: hidden;
}
</style>