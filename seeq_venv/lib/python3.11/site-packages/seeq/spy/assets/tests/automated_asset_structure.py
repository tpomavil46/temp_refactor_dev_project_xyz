
from seeq.spy.assets import Asset


class Plant(Asset):

    @Asset.Component()
    def Turbines(self, metadata):
        return self.build_components(Turbine, metadata, column_name='Turbine')


class Turbine(Asset):

    @Asset.Component()
    def Gearbox(self, metadata):
        return self.build_components(Gearbox, metadata, column_name='Gearbox')

    @Asset.Attribute()
    def Gearbox_Meta_Warning_Value(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Meta Warning Value']

    @Asset.Attribute()
    def Gearbox_Meta_Warning_MA(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Meta Warning MA']

    @Asset.Attribute()
    def Gearbox_OMR(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox OMR']

    @Asset.Component()
    def Generator(self, metadata):
        return self.build_components(Generator, metadata, column_name='Generator')

    @Asset.Attribute()
    def Generator_Meta_Warning_Value(self, metadata):
        return metadata[metadata['Name'] == 'Generator Meta Warning Value']

    @Asset.Attribute()
    def Generator_Meta_Warning_MA(self, metadata):
        return metadata[metadata['Name'] == 'Generator Meta Warning MA']

    @Asset.Attribute()
    def Generator_OMR(self, metadata):
        return metadata[metadata['Name'] == 'Generator OMR']

    @Asset.Component()
    def Main_Bearing(self, metadata):
        return self.build_components(Main_Bearing, metadata, column_name='Main Bearing')

    @Asset.Attribute()
    def Main_Bearing_Meta_Warning_Value(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing Meta Warning Value']

    @Asset.Attribute()
    def Main_Bearing_Meta_Warning_MA(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing Meta Warning MA']

    @Asset.Attribute()
    def Main_Bearing_OMR(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing OMR']


class Gearbox(Asset):

    @Asset.Attribute()
    def Scheduled_Outage(self, metadata):
        return metadata[metadata['Name'] == 'Scheduled Outage Manual Adjustment']

    @Asset.Attribute()
    def Curtailment(self, metadata):
        return metadata[metadata['Name'] == 'Curtailment Active Site AF']

    @Asset.Attribute()
    def Expected_Power(self, metadata):
        return metadata[metadata['Name'] == 'Expected Power While Running']

    @Asset.Attribute()
    def Flatline(self, metadata):
        return metadata[metadata['Name'] == 'Turbine Flatline']

    @Asset.Attribute()
    def Max_Power(self, metadata):
        return metadata[metadata['Name'] == 'Curve Expected Power']

    @Asset.Attribute()
    def Active_Power(self, metadata):
        return metadata[metadata['Name'] == 'Active Power - Raw']

    @Asset.Attribute()
    def Parameter_Warning_Gearbox_Bearing_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Gearbox Bearing Temp']

    @Asset.Attribute()
    def Gearbox_HSS_Brg_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox_HSS_Brg_Temp']

    @Asset.Attribute()
    def Parameter_Warning_Gearbox_Oil_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Gearbox Oil Temp']

    @Asset.Attribute()
    def Gearbox_Oil_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Oil Temp']

    @Asset.Attribute()
    def Parameter_Warning_Oil_Temp_Model(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Oil Temp Model']

    @Asset.Attribute()
    def Gearbox_Oil_Temp_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Oil Temp Residuals']

    @Asset.Attribute()
    def Parameter_Warning_Bearing_Temp_Model(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Bearing Temp Model']

    @Asset.Attribute()
    def Gearbox_HSS_Brg_Temp_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox_HSS_Brg_Temp Residuals']

    @Asset.Attribute()
    def Parameter_Warning_Gearbox_Vibration_HSS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Gearbox Vibration HSS Direct RMS']

    @Asset.Attribute()
    def Gearbox_Vibration_HSS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration HSS Direct RMS']

    @Asset.Attribute()
    def Parameter_Warning_Gearbox_Vibration_PS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Gearbox Vibration PS Direct RMS']

    @Asset.Attribute()
    def Gearbox_Vibration_PS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration PS Direct RMS']

    @Asset.Attribute()
    def Parameter_Warning_Gearbox_Vibration_IMS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Gearbox Vibration IMS Direct RMS']

    @Asset.Attribute()
    def Gearbox_Vibration_IMS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration IMS Direct RMS']

    @Asset.Attribute()
    def Parameter_Warning_Vibration_HSS(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Vibration HSS']

    @Asset.Attribute()
    def Gearbox_Vibration_HSS_Direct_RMS_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration HSS Direct RMS Residuals']

    @Asset.Attribute()
    def Parameter_Warning_Vibration_IMS(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Vibration IMS']

    @Asset.Attribute()
    def Gearbox_Vibration_IMS_Direct_RMS_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration IMS Direct RMS Residuals']

    @Asset.Attribute()
    def Parameter_Warning_Vibration_PS(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Vibration PS']

    @Asset.Attribute()
    def Gearbox_Vibration_PS_Direct_RMS_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration PS Direct RMS Residuals']

    @Asset.Attribute()
    def Parameter_Warning_Total_Mass(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Total Mass']

    @Asset.Attribute()
    def Total_Mass(self, metadata):
        return metadata[metadata['Name'] == 'Total Mass']

    @Asset.Attribute()
    def WindSpeed(self, metadata):
        return metadata[metadata['Name'] == 'WindSpeed']

    @Asset.Attribute()
    def Ambient_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Ambient_Temp']

    @Asset.Attribute()
    def Rotor_Speed(self, metadata):
        return metadata[metadata['Name'] == 'Rotor_Speed']

    @Asset.Attribute()
    def Gen_Speed(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Speed']

    @Asset.Attribute()
    def Gearbox_Vibration_HSS_Voltage_Bias(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration HSS Voltage Bias']

    @Asset.Attribute()
    def Gearbox_Vibration_IMS_Voltage_Bias(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration IMS Voltage Bias']

    @Asset.Attribute()
    def Gearbox_Vibration_PS_Voltage_Bias(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration PS Voltage Bias']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_0_to_100_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (0 - 100 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_101_to_200_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (101 - 200 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_201_to_300_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (201 - 300 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_301_to_400_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (301 - 400 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_401_to_500_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (401 - 500 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_501_to_600_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (501 - 600 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_601_to_700_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (601 - 700 Microns)']

    @Asset.Attribute()
    def Particle_Counter_Ferrous_700plus_Microns(self, metadata):
        return metadata[metadata['Name'] == 'Particle Counter Ferrous (700+ Microns)']

    @Asset.Attribute()
    def Predicted_Gearbox_HSS_Brg_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gearbox_HSS_Brg_Temp']

    @Asset.Attribute()
    def Gearbox_HSS_Brg_Temp_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox_HSS_Brg_Temp Warning']

    @Asset.Attribute()
    def Temperatures_Training(self, metadata):
        return metadata[metadata['Name'] == 'Temperatures Training Data']

    @Asset.Attribute()
    def Predicted_Gearbox_Oil_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gearbox Oil Temp']

    @Asset.Attribute()
    def Gearbox_Oil_Temp_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Oil Temp Warning']

    @Asset.Attribute()
    def Predicted_Gearbox_Vibration_HSS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gearbox Vibration HSS Direct RMS']

    @Asset.Attribute()
    def Gearbox_Vibration_HSS_Direct_RMS_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration HSS Direct RMS Warning']

    @Asset.Attribute()
    def Vibrations_Training(self, metadata):
        return metadata[metadata['Name'] == 'Vibrations Training Data']

    @Asset.Attribute()
    def Predicted_Gearbox_Vibration_IMS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gearbox Vibration IMS Direct RMS']

    @Asset.Attribute()
    def Gearbox_Vibration_IMS_Direct_RMS_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration IMS Direct RMS Warning']

    @Asset.Attribute()
    def Predicted_Gearbox_Vibration_PS_Direct_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gearbox Vibration PS Direct RMS']

    @Asset.Attribute()
    def Gearbox_Vibration_PS_Direct_RMS_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gearbox Vibration PS Direct RMS Warning']

    @Asset.Attribute()
    def Retraining_Event(self, metadata):
        return metadata[metadata['Name'] == 'Retraining Event']

    @Asset.Attribute()
    def Low(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning Low']

    @Asset.Attribute()
    def Med(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning Med']

    @Asset.Attribute()
    def High(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning High']


class Generator(Asset):

    @Asset.Attribute()
    def Generator_Speed(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Speed']

    @Asset.Attribute()
    def Winding_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Generator Winding 1 Temperature']

    @Asset.Attribute()
    def Cooling_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Gen Cooling Temperature']

    @Asset.Attribute()
    def Parameter_Warning_Generator_Winding_2_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Generator Winding 2 Temperature']

    @Asset.Attribute()
    def Generator_Winding_2_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Generator Winding 2 Temperature']

    @Asset.Attribute()
    def Parameter_Warning_Gen_Cooling_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Gen Cooling Temperature']

    @Asset.Attribute()
    def Parameter_Warning_Generator_Winding_1_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Generator Winding 1 Temperature']

    @Asset.Attribute()
    def Parameter_Warning_Electrical_Imbalance(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Electrical Imbalance']

    @Asset.Attribute()
    def Phase_Voltage_A_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage A Residuals']

    @Asset.Attribute()
    def Phase_Voltage_B_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage B Residuals']

    @Asset.Attribute()
    def Phase_Voltage_C_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage C Residuals']

    @Asset.Attribute()
    def Phase_Current_A_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current A Residuals']

    @Asset.Attribute()
    def Phase_Current_B_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current B Residuals']

    @Asset.Attribute()
    def Phase_Current_C_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current C Residuals']

    @Asset.Attribute()
    def Parameter_Warning_Generator_Outboard_Vibration(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Generator Outboard Vibration']

    @Asset.Attribute()
    def Generator_Inboard_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Generator Inboard RMS']

    @Asset.Attribute()
    def Parameter_Warning_Generator_Inboard_Vibration(self, metadata):
        return metadata[metadata['Name'] == 'Parameter Warning Generator Inboard Vibration']

    @Asset.Attribute()
    def Generator_Outboard_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Generator Outboard RMS']

    @Asset.Attribute()
    def Active_Power_to_Raw(self, metadata):
        return metadata[metadata['Name'] == 'Active Power - Raw']

    @Asset.Attribute()
    def Ambient_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Ambient_Temp']

    @Asset.Attribute()
    def WindSpeed(self, metadata):
        return metadata[metadata['Name'] == 'WindSpeed']

    @Asset.Attribute()
    def Generator_Reactive_Power_VARs(self, metadata):
        return metadata[metadata['Name'] == 'Generator Reactive Power (VARs)']

    @Asset.Attribute()
    def Turbine_Frequency(self, metadata):
        return metadata[metadata['Name'] == 'Turbine Frequency']

    @Asset.Attribute()
    def Phase_Voltage_A(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage A']

    @Asset.Attribute()
    def Phase_Voltage_B(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage B']

    @Asset.Attribute()
    def Phase_Voltage_C(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage C']

    @Asset.Attribute()
    def Phase_Current_A(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current A']

    @Asset.Attribute()
    def Phase_Current_B(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current B']

    @Asset.Attribute()
    def Phase_Current_C(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current C']

    @Asset.Attribute()
    def Predicted_Gen_Brg_NDE_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gen_Brg_NDE_Temp']

    @Asset.Attribute()
    def Gen_Brg_NDE_Temp_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Brg_NDE_Temp Residuals']

    @Asset.Attribute()
    def Gen_Brg_NDE_Temp_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Brg_NDE_Temp Warning']

    @Asset.Attribute()
    def Temperatures_Training(self, metadata):
        return metadata[metadata['Name'] == 'Temperatures Training Data']

    @Asset.Attribute()
    def Gen_Brg_NDE_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Brg_NDE_Temp']

    @Asset.Attribute()
    def Predicted_Gen_Brg_DE_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gen_Brg_DE_Temp']

    @Asset.Attribute()
    def Gen_Brg_DE_Temp_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Brg_DE_Temp Residuals']

    @Asset.Attribute()
    def Gen_Brg_DE_Temp_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Brg_DE_Temp Warning']

    @Asset.Attribute()
    def Gen_Brg_DE_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Gen_Brg_DE_Temp']

    @Asset.Attribute()
    def Predicted_Gen_Cooling_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Gen Cooling Temperature']

    @Asset.Attribute()
    def Gen_Cooling_Temperature_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Gen Cooling Temperature Residuals']

    @Asset.Attribute()
    def Gen_Cooling_Temperature_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Gen Cooling Temperature Warning']

    @Asset.Attribute()
    def Predicted_Generator_Winding_1_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Generator Winding 1 Temperature']

    @Asset.Attribute()
    def Generator_Winding_1_Temperature_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Generator Winding 1 Temperature Residuals']

    @Asset.Attribute()
    def Generator_Winding_1_Temperature_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Generator Winding 1 Temperature Warning']

    @Asset.Attribute()
    def Predicted_Generator_Winding_2_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Generator Winding 2 Temperature']

    @Asset.Attribute()
    def Generator_Winding_2_Temperature_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Generator Winding 2 Temperature Residuals']

    @Asset.Attribute()
    def Generator_Winding_2_Temperature_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Generator Winding 2 Temperature Warning']

    @Asset.Attribute()
    def Predicted_Generator_Inboard_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Generator Inboard RMS']

    @Asset.Attribute()
    def Generator_Inboard_RMS_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Generator Inboard RMS Residuals']

    @Asset.Attribute()
    def Generator_Inboard_RMS_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Generator Inboard RMS Warning']

    @Asset.Attribute()
    def Vibrations_Training(self, metadata):
        return metadata[metadata['Name'] == 'Vibrations Training Data']

    @Asset.Attribute()
    def Predicted_Generator_Outboard_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Generator Outboard RMS']

    @Asset.Attribute()
    def Generator_Outboard_RMS_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Generator Outboard RMS Residuals']

    @Asset.Attribute()
    def Generator_Outboard_RMS_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Generator Outboard RMS Warning']

    @Asset.Attribute()
    def Predicted_Phase_Voltage_A(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Phase Voltage A']

    @Asset.Attribute()
    def Phase_Voltage_A_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage A Warning']

    @Asset.Attribute()
    def Electrical_Training(self, metadata):
        return metadata[metadata['Name'] == 'Electrical Training Data']

    @Asset.Attribute()
    def Predicted_Phase_Voltage_B(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Phase Voltage B']

    @Asset.Attribute()
    def Phase_Voltage_B_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage B Warning']

    @Asset.Attribute()
    def Predicted_Phase_Voltage_C(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Phase Voltage C']

    @Asset.Attribute()
    def Phase_Voltage_C_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Phase Voltage C Warning']

    @Asset.Attribute()
    def Predicted_Phase_Current_A(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Phase Current A']

    @Asset.Attribute()
    def Phase_Current_A_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current A Warning']

    @Asset.Attribute()
    def Predicted_Phase_Current_B(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Phase Current B']

    @Asset.Attribute()
    def Phase_Current_B_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current B Warning']

    @Asset.Attribute()
    def Predicted_Phase_Current_C(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Phase Current C']

    @Asset.Attribute()
    def Phase_Current_C_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Phase Current C Warning']

    @Asset.Attribute()
    def Retraining_Event(self, metadata):
        return metadata[metadata['Name'] == 'Retraining Event']

    @Asset.Attribute()
    def Low(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning Low']

    @Asset.Attribute()
    def Med(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning Med']

    @Asset.Attribute()
    def High(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning High']


class Main_Bearing(Asset):

    @Asset.Attribute()
    def User_Set_Warning_for_Main_Bearing_RMS(self, metadata):
        return metadata[metadata['Name'] == 'User Set Warning for Main Bearing RMS']

    @Asset.Attribute()
    def Main_Bearing_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing RMS']

    @Asset.Attribute()
    def User_Set_Warning_for_Main_Bearing_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'User Set Warning for Main Bearing Temperature']

    @Asset.Attribute()
    def Main_Bearing_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing Temperature']

    @Asset.Attribute()
    def Active_Power_to_Raw(self, metadata):
        return metadata[metadata['Name'] == 'Active Power - Raw']

    @Asset.Attribute()
    def WindSpeed(self, metadata):
        return metadata[metadata['Name'] == 'WindSpeed']

    @Asset.Attribute()
    def Ambient_Temp(self, metadata):
        return metadata[metadata['Name'] == 'Ambient_Temp']

    @Asset.Attribute()
    def Rotor_Speed(self, metadata):
        return metadata[metadata['Name'] == 'Rotor_Speed']

    @Asset.Attribute()
    def Predicted_Main_Bearing_RMS(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Main Bearing RMS']

    @Asset.Attribute()
    def Main_Bearing_RMS_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing RMS Residuals']

    @Asset.Attribute()
    def Main_Bearing_RMS_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing RMS Warning']

    @Asset.Attribute()
    def Vibrations_Training(self, metadata):
        return metadata[metadata['Name'] == 'Vibrations Training Data']

    @Asset.Attribute()
    def Predicted_Main_Bearing_Temperature(self, metadata):
        return metadata[metadata['Name'] == 'Predicted Main Bearing Temperature']

    @Asset.Attribute()
    def Main_Bearing_Temperature_Residuals(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing Temperature Residuals']

    @Asset.Attribute()
    def Main_Bearing_Temperature_Warning(self, metadata):
        return metadata[metadata['Name'] == 'Main Bearing Temperature Warning']

    @Asset.Attribute()
    def Temperatures_Training(self, metadata):
        return metadata[metadata['Name'] == 'Temperatures Training Data']

    @Asset.Attribute()
    def Retraining_Event(self, metadata):
        return metadata[metadata['Name'] == 'Retraining Event']

    @Asset.Attribute()
    def Low(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning Low']

    @Asset.Attribute()
    def Med(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning Med']

    @Asset.Attribute()
    def High(self, metadata):
        return metadata[metadata['Name'] == 'Meta Warning High']
