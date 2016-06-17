package com.em.bdmp.framework.jobmanage.task;

import java.net.URLEncoder;
import java.util.Date;
import java.util.List;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;

import com.em.bdmp.common.constants.SysConstant;
import com.em.bdmp.common.kettle.HttpKettle;
import com.em.bdmp.common.kettle.TransStatus;
import com.em.bdmp.common.kettle.WebResult;
import com.em.bdmp.common.spring.EMailSerder;
import com.em.bdmp.common.spring.SpringContextHolder;
import com.em.bdmp.framework.jobmanage.entity.JobInfo;
import com.em.bdmp.framework.jobmanage.entity.JobServer;
import com.em.bdmp.framework.jobmanage.service.JobInfoService;
import com.em.bdmp.framework.jobmanage.service.JobServerService;

/**
 * 
 * @author zhangtao kettle Transformaction 任务调度
 *
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class KettleTransTask implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		// 初始化 
		JobDataMap map = context.getJobDetail().getJobDataMap();
		map.put(SysConstant.QRTZ_SYS_OUT, "");
		map.put(SysConstant.QRTZ_SYS_ERR, "");
		String jobKey = (context.getJobDetail().getKey()).toString();
		//获取任务信息
		JobInfoService jobInfoService = SpringContextHolder.getBean("jobInfoServiceImpl");
		JobInfo jobinfo = jobInfoService.getJobInfo(jobKey);
		if (jobinfo == null || jobinfo.getId() == null || "".equals(jobinfo.getId())) {
			map.put(SysConstant.QRTZ_SYS_ERR, "[异常] 获取JobInfo信息失败");
			return;
		}
		StringBuffer sb = new StringBuffer("[正常] 获取jobInfo信息成功，任务名：" + jobinfo.getJobEName() + ", 程序名：" + jobinfo.getProgramName() + "\n");
		JobServerService jobServerService = SpringContextHolder.getBean("jobServerServiceImpl");
		JobServer jobServer = jobServerService.getJobServer((jobinfo.getServId()).trim());
		
		if (jobServer == null || jobServer.getId() == null || "".equals(jobServer.getId())) {
			sb.append("[异常] 获取jobServer信息失败.");
			map.put(SysConstant.QRTZ_SYS_ERR, sb.toString());
			EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", " ERROR信息 ：获取jobServer信息失败.");
			return;
		}
		sb.append("[正常] 获取jobServer信息成功，服务名称：" + jobServer.getServName() + ", 服务地址：" + jobServer.getServIP() + "\n");
		String responseBody = "";
		String service ="";
		try{
			Long startTime = (new Date()).getTime();
			sb.append("[正常] 开始调用......\n");
			// 开始运行Trans节点
			// NOTHING( 0, "Nothing" ), ERROR( 1, "Error" ), MINIMAL( 2, "Minimal" ), BASIC( 3, "Basic" ), DETAILED( 4, "Detailed" ), DEBUG( 5, "Debug" ), ROWLEVEL( 6, "Rowlevel" );
			service = HttpKettle.CONTEXT_RUNTRANS + "/?trans=" + URLEncoder.encode(jobinfo.getProgramPath() + "/", "UTF-8")
					+ URLEncoder.encode(jobinfo.getProgramName(), "UTF-8") + "&level=Basic";
				responseBody = HttpKettle.execService(service, jobServer);
			
			if(("".equals(responseBody) || responseBody.contains("Exception:")) && "T".equals(jobinfo.getIsSpare())){
				if(responseBody.contains("Connection") || responseBody.contains("拒绝连接")){
					sb.append("[异常] 程序连 接" + jobServer.getServIP() + " 服务器异常：" + responseBody + "\n 程序切换其他服务器......\n");
					List<JobServer> list = jobServerService.getJobServerList();
					for(JobServer entity : list){
						if(!((jobinfo.getServId()).trim()).equals(entity.getId())){
							responseBody = HttpKettle .execService(service, entity);
							if(responseBody.contains("Connection") || responseBody.contains("拒绝连接")) {
								sb.append("[异常] 程序连 接" + entity.getServIP() + " 服务器异常：" + responseBody + "\n 程序切换其他服务器......\n");
								continue;
							}else if(!"".equals(responseBody) && !responseBody.contains("Exception:")){
								jobServer = entity;
								sb.append("[正常] 获取jobServer信息成功，服务名称：" + jobServer.getServName() + ", 服务地址：" + jobServer.getServIP() + "\n");
								break;
							}else{
								sb.append("[异常] 程序连 接" + entity.getServIP() + " 服务器异常：" + responseBody + "\n 程序切换其他服务器......\n");
							}
						}
					}
				}else{
					sb.append("[异常] 程序异常结束, 请检查." + responseBody);
					map.put(SysConstant.QRTZ_SYS_ERR, sb.toString());
					EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", " ERROR信息 ：" + responseBody);
					return;
				}
			}
			if("".equals(responseBody) || responseBody.contains("Exception:")){
				sb.append("[异常] 程序异常结束, 请检查." + responseBody);
				map.put(SysConstant.QRTZ_SYS_ERR, sb.toString());
				EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", " ERROR信息 ：" + responseBody);
				return;
			}
			
			WebResult webResult = WebResult.fromXMLString(responseBody);
			if (!webResult.getResult() .equalsIgnoreCase(WebResult.STRING_OK)) {
				EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", " ERROR信息 ：There was an error posting the transformation on the remote server: " + webResult.getMessage());
				throw new Exception( "There was an error posting the transformation on the remote server: " + webResult.getMessage());
			}
			String carteObjectId = webResult.getId();
			sb.append("[正常] 调用成功，任务Id" + carteObjectId + "\n");

			// 检查Trans节点运行状态
			sb.append("[正常] 开始监控任务执行状态......\n");
			service = HttpKettle.CONTEXT_TANSSTATUS + "/?name=" + URLEncoder.encode(jobinfo.getProgramName(), "UTF-8") + "&id=" + carteObjectId + "&xml=Y";
			boolean falg = true;
			do{
				responseBody = HttpKettle.execService(service, jobServer);
				TransStatus transStatus = TransStatus.fromXMLString(responseBody);
				
				if(responseBody.contains("ERROR") && responseBody.contains("could not be found")){
					falg = false;
					sb.append("[正常] 任务已完成。");
					map.put(SysConstant.QRTZ_SYS_OUT, sb.toString());
				}
				
				if("Waiting".equalsIgnoreCase(transStatus.getStatusDescription())){
					service = HttpKettle.CONTEXT_REMOVETRANS + "/?name=" + URLEncoder.encode(jobinfo.getProgramName(), "UTF-8") + "&id=" + carteObjectId + "&xml=Y";
					HttpKettle.execService(service, jobServer);
					falg = false;
					sb.append("[正常] 任务已完成。");
					map.put(SysConstant.QRTZ_SYS_OUT, sb.toString());
				}
				
				if("Stop".equalsIgnoreCase(transStatus.getStatusDescription())){
					service = HttpKettle.CONTEXT_REMOVETRANS + "/?name=" + URLEncoder.encode(jobinfo.getProgramName(), "UTF-8") + "&id=" + carteObjectId + "&xml=Y";
					HttpKettle.execService(service, jobServer);
					String errorinfo = "ERROR信息 ：" + transStatus.getErrorDescription() + "\n 日志内容(" + transStatus.getFirstLoggingLineNr() + "-" + transStatus.getLastLoggingLineNr() + ")： " + transStatus.getLoggingString();
					EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", errorinfo);
					falg = false;
					sb.append("[异常] 程序最终结束状态为 Stop, 请检查." + errorinfo);
					map.put(SysConstant.QRTZ_SYS_ERR, sb.toString());
				}
				
				// 程序运行超时判断
				if(jobinfo.getTimeOutRemind() != null && jobinfo.getTimeOutRemind() > 0){
					Long endTime = (new Date()).getTime();
					if((endTime - startTime) > (jobinfo.getTimeOutRemind() * 1000)){
						service = HttpKettle.CONTEXT_REMOVETRANS + "/?name=" + URLEncoder.encode(jobinfo.getProgramName(), "UTF-8") + "&id=" + carteObjectId + "&xml=Y";
						HttpKettle.execService(service, jobServer);
						falg = false;
						sb.append("[异常] 程序执行超时, 超时阀值(秒)：" + jobinfo.getTimeOutRemind());
						EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", "程序执行超时！超时阀值(秒)：" + jobinfo.getTimeOutRemind());
						map.put(SysConstant.QRTZ_SYS_OUT, sb.toString());
					}
				}
				
				Thread.sleep(3*1000);
			}while(falg);
			
			//执行程序等待时间
			if(jobinfo.getWaitTime() != null && jobinfo.getWaitTime() > 0){
				Thread.sleep(jobinfo.getWaitTime() * 1000);
			}
		} catch (Exception e) {
			sb.append("[异常] 程序异常结束, 请检查." + e.getMessage());
			map.put(SysConstant.QRTZ_SYS_ERR, sb.toString());
			EMailSerder.send(jobinfo.getMailAddress(), (jobinfo.getProgramName()).trim() + " 程序异常", "ERROR信息 ：" + e.getMessage());
			e.printStackTrace();
		}
	}
}