% Instructions
% In the command prompt type:
% [output markers CRT errmsg listselection PathName FileName PatientData] = readBeatscopeData20
% Select ALL the data in the pop-up window
% The following lines of code will read the output structure and create a
% txt file where the first column is time and the second column is values
% of the parameter of interest from Finapres specific to our company.

B19013Test2fiAP(:,1)=output.fiAP.time;
B19013Test2fiAP(:,2)=output.fiAP.values;
save B19013Test2fiAP_Data.dat B19013Test2fiAP -ascii

B19013Test2HR_AP(:,1)=output.HR_AP.time;
B19013Test2HR_AP(:,2)=output.HR_AP.values;
save B19013Test2HR_AP_Data.dat B19013Test2HR_AP -ascii

B19013Test2IBI(:,1)=output.IBI.time;
B19013Test2IBI(:,2)=output.IBI.values;
save B19013Test2IBI_Data.dat B19013Test2IBI -ascii

B19013Test2ECG_II(:,1)=output.ECG_II.time;
B19013Test2ECG_II(:,2)=output.ECG_II.values;
save B19013Test2ECG_II_Data.dat B19013Test2ECG_II -ascii

B19013Test2RR_int(:,1)=output.RR_int.time;
B19013Test2RR_int(:,2)=output.RR_int.values;
save B19013Test2RR_int_Data.dat B19013Test2RR_int -ascii

B19013Test2reBAP(:,1)=output.reBAP.time;
B19013Test2reBAP(:,2)=output.reBAP.values;
save B19013Test2reBAP_Data.dat B19013Test2reBAP -ascii

B19013Test2PhysioCalActive(:,1)=output.PhysioCalActive.time;
B19013Test2PhysioCalActive(:,2)=output.PhysioCalActive.values;
save B19013Test2PhysioCalActive_Data.dat B19013Test2PhysioCalActive -ascii

B19013Test2reSYS(:,1)=output.reSYS.time;
B19013Test2reSYS(:,2)=output.reSYS.values;
save B19013Test2reSYS_Data.dat B19013Test2reSYS -ascii

B19013Test2reDIA(:,1)=output.reDIA.time;
B19013Test2reDIA(:,2)=output.reDIA.values;
save B19013Test2reDIA_Data.dat B19013Test2reDIA -ascii


B19013Test2reMAP(:,1)=output.reMAP.time;
B19013Test2reMAP(:,2)=output.reMAP.values;
save B19013Test2reMAP_Data.dat B19013Test2reMAP -ascii

B19013Test2fiSYS(:,1)=output.fiSYS.time;
B19013Test2fiSYS(:,2)=output.fiSYS.values;
save B19013Test2fiSYSData.dat B19013Test2fiSYS -ascii

B19013Test2fiDIA(:,1)=output.fiDIA.time;
B19013Test2fiDIA(:,2)=output.fiDIA.values;
save B19013Test2fiDIA_Data.dat B19013Test2fiDIA -ascii

B19013Test2fiMAP(:,1)=output.fiMAP.time;
B19013Test2fiMAP(:,2)=output.fiMAP.values;
save B19013Test2fiMAP_Data.dat B19013Test2fiMAP -ascii

B19013Test2HR(:,1)=output.HR.time;
B19013Test2HR(:,2)=output.HR.values;
save B19013Test2HR_Data.dat B19013Test2HR -ascii
clf       % clears all figures
subplot(2,1,1);
hold on   % Several plots will be plotted on a single figure
plot1=plot(output.reSYS.time,output.reSYS.values) % Plots time versus recalibrated systolic bp
plot2=plot(output.reDIA.time,output.reDIA.values) % Plots time versus recalibrated systolic bp


ApproxStartToStandTime=input('Enter start to stand time in the markers structure: ');
xmin=ApproxStartToStandTime-700;  % This will capture time 10 minutes before start of standing
xmax=ApproxStartToStandTime+300;  % This will capture time 5 minutes after start of standing

for count=1:250                   % This for loop is to create a vertical line on the plot approximately
    xStartToStandTime(count,1)=ApproxStartToStandTime; % at the time the participant stands
    yStartToStandTimeValues(count,1)=count;
end

% This will print the vertical line approx when participant started to stand
plot(xStartToStandTime,yStartToStandTimeValues,'r') 
grid minor
txt1 = '\rightarrow Approx Starting to Stand time (sec):';  % Print a right arrow
text(ApproxStartToStandTime,25,txt1)                          % label the vertical line 
txt2=num2str(ApproxStartToStandTime) % num2str converts a number to a string
text(ApproxStartToStandTime,15,txt2) % print txt2 string at the x coord of ApproxStartToStandTime

legend([plot1 plot2],{'reSYS','reDIA'})    % Create a legend on the plot
axis([xmin xmax 0 250])                    % Establish the x and y axes min and max values
xlabel('Time (sec)')
ylabel('reSYS and reDIA Blood Pressures (mmHg)')
title({'B19013Test2';FileName})

subplot(2,1,2);
hold on
grid minor
plot(output.HR.time,output.HR.values)
plot(xStartToStandTime,yStartToStandTimeValues,'r') 
maxHR=max(output.HR.values);
minHR=min(output.HR.values);
axis([xmin xmax minHR-10 maxHR+10])
% axis([xmin xmax minHR-10 120])
ylabel('HR (beats/min)')
xlabel('Time (sec)')

print('B19013Test2_SYS_DIA_HR','-dpng')