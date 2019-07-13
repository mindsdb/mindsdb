function [output markers CRT errmsg listselection PathName FileName PatientData] = readBeatscopeData20(FileName, signalnames)
% [output markers CRT errmsg listselection PathName FileName PatientData] = readBeatscopeData20(FileName,signalnames)
%
% IN:  filename,    measurement file
%                         if 'help' or '?' is given as filename, this help instruction will be showed 
%      signalnames, cell array of signalnames (optional, if none provided an overview will pop up); use {'all'} to get all signals
%
% OUT: output,      a struct containing for each signal its time and values
%                   vector. It also contains a description of the signal (type, units, samplerate)
%                   If no filename is provided and no file to open is
%                   selected (cancel is pressed in the opening dialog),
%                   output is an emtpy matrix. 
%      markers      struct with markers containing a time and label subfield
%      CRT          struct with CRT data (empty if the file is not CRT): 
%                     - Some booleans: indicating protocol finish,
%                     biventricular pacing, 'usepacingspikes'
%                     - Optimal av delay, 95% CI lower limit, 95% CI upper
%                     limit, scatter, curvature of fit
%                     - protocol steps, per step: 
%                       - reference and test delay, 
%                       - transitions, per transition: 
%                           - absolute changeover time, 
%                           - average SYS before, and after transition
%                           - SYS data before and after transition per
%                           beat. 
%      errmsg,      error message
%      listselection, a list of signalnames that can be used to retrieve
%                     data from the output struct. Please be advised that
%                     non-compatible characters for structfields are
%                     replaced by an underscore ( _ ).
%      PathName     the path name of the file that is read. If cancel is
%                   pressed when selecting file to read, PathName = 0. 
%      FileName     returns the filename of the chosen file. If cancel is
%                   pressed when selecting file to read, FileName = 0. 
%      PatientData  Patient demographics and user comments as a struct
%
% This function reads measurement files acquired using the Finapres Nova. 
%
% An additional input with signalnames will result in cell arrays of time
% and value vectors. If none are provided a selection dialog will be shown
% 
% If no filename is given a browser window will be opened
% 
% Example call: 
% [output markers CRT errmsg listselection PathName FileName PatientData] = readBeatscopeData20([], {'ECG I''fiAP'}); 
% gives in output the raw data of ECGI and the finger arterial pressure, all
% included markers, possible error messages occurring while reading the
% file and feedback on what data fields have been selected. CRT data is
% only given is a CRT TrueMax file has been loaded.
% Example call"
% Simply copy-paste the following into the matlab command window and press enter:
%
% [output markers CRT errmsg listselection PathName FileName PatientData] = readBeatscopeData20
%
% This will subsequently ask you for a file to open. 
% Then it will ask you which signals to extract (listbox where you can select multiple parameters).
% Upon pressing OK the file will be read and this will result in the variables between brackets in the workspace.
% When you type help readBeatscopeData20 a help-instruction will be
% presented in the matlab command window.
