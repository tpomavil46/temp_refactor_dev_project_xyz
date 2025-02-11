import os

import pandas as pd
import pytest

from seeq import spy
from seeq.spy.assets import Asset, ItemGroup


#
#  This test file is a result of an investigation kicked off by Sharlinda as documented in CRAB-32080.
#  Since this is a nice test case (it depends only on a CSV file for input), Mark D decided to include it wholesale
#  as a unit test.
#


# noinspection PyPep8Naming
class Prototype_Mass_Balance(Asset):

    @Asset.Component()
    def Regionals(self, metadata):
        return self.build_components(template=Regional, metadata=metadata, column_name='Regional')


# noinspection PyPep8Naming
class Regional(Asset):

    @Asset.Component()
    def Sites(self, metadata):
        return self.build_components(template=Site, metadata=metadata, column_name='Site')


# noinspection PyPep8Naming
class Site(Asset):

    @Asset.Component()
    def Units(self, metadata):
        return self.build_components(template=Unit, metadata=metadata, column_name='Unit')

    @Asset.Attribute()
    def Total_Site_kEDC(self, metadata):
        return ItemGroup([
            asset.kEdC_Biweeks_Possible() for asset in self.all_assets()
            if asset.is_descendant_of(self) and asset.definition['Template'] == "Unit"  # CRAB-30512
        ]).roll_up('sum')


# noinspection PyPep8Naming
class Unit(Asset):

    @Asset.Attribute()
    def Mass_Balance(self, metadata):
        Mass_metadata = metadata[metadata['Asset Tree Name'].str.contains('Mass')]
        if not Mass_metadata.empty:
            return {
                'Name': '00. Mass Balance',
                'Type': 'Signal',
                'Formula': '$MB',
                'Formula Parameters': {'$MB': Mass_metadata.iloc[0]['ID']}
            }

    @Asset.Attribute()
    def kEDC(self, metadata):
        kEDC_metadata = metadata[metadata['Asset Tree Name'].str.contains('kEDC')]
        if not kEDC_metadata.empty:
            return {
                'Name': '00. kEDC',
                'Type': 'Signal',
                'Formula': '$kEDC',
                'Formula Parameters': {'$kEDC': kEDC_metadata.iloc[0]['ID']}
            }

    @Asset.Attribute()
    def Filtered(self, metadata):
        Filtered_metadata = metadata[metadata['Asset Tree Name'].str.contains('Filtered')]
        if not Filtered_metadata.empty:
            return {
                'Name': '00. Filtered',
                'Type': 'Condition',
                'Formula': '$Filtered',
                'Formula Parameters': {'$Filtered': Filtered_metadata.iloc[0]['ID']}
            }

    @Asset.Attribute()
    def Sampling(self, metadata):
        Sampling_metadata = metadata[metadata['Asset Tree Name'].str.contains('Sampling')]
        if not Sampling_metadata.empty:
            return {
                'Name': '00. Sampling Frequency',
                'Type': 'Condition',
                'Formula': '$sampling_frequency',
                'Formula Parameters': {'$sampling_frequency': Sampling_metadata.iloc[0]['ID']}
            }

    @Asset.Attribute()
    def Quarter_Property(self, metadata):
        return {
            'Name': '1.0 Quarter_property',
            'Type': 'Condition',
            'Formula': f'//default property of "Year" and "Quarter" are embedded by default\n'
                       f'//trending the property as string signal\n'
                       f'$year    = years("Europe/Berlin").move(-2h).toSignal("Year").toString()\n'
                       f'$quarter = quarters("Europe/Berlin").move(-2h).toSignal("Quarter").toString()\n'
                       f'//move(-2h) to be consistent with sampling frequency\n'
                       f'//creating signal eg 2020 Q3\n'
                       f'$signal = $year + " Q" + $quarter\n'
                       f'//The default property name when using toCondition is Value\n'
                       f'//Change the default name to YYYY QQ\n'
                       f'$year_quarter = $signal.toCondition("YYYY QQ").removeLongerThan(95d)\n'
                       f'$year_quarter'
        }

    @Asset.Attribute()
    def Out_of_Tolerance(self, metadata):
        Unique_tolerance_metadata = metadata.dropna(axis=0, subset={'Unique Tolerance'})
        if not Unique_tolerance_metadata.empty:
            return {
                'Name': '1.1 Out of Tolerance',
                'Type': 'Condition',
                'Formula': f'$sample = $MB.toDiscrete().toCapsules().keep("Value",isNotBetween(105,95))\n'
                           f'$sampling_frequency.touches($sample)',
                'Formula Parameters': {'$MB': self.Mass_Balance(),
                                       '$sampling_frequency': self.Sampling()}
            }
        else:
            return {
                'Name': '1.1 Out of Tolerance',
                'Type': 'Condition',
                'Formula': f'$sample = $MB.toDiscrete().toCapsules().keep("Value",isNotBetween(103,97))\n'
                           f'$sampling_frequency.touches($sample)',
                'Formula Parameters': {'$MB': self.Mass_Balance(),
                                       '$sampling_frequency': self.Sampling()}
            }

    @Asset.Attribute()
    def In_Tolerance(self, metadata):
        Unique_tolerance_metadata = metadata.dropna(axis=0, subset={'Unique Tolerance'})
        if not Unique_tolerance_metadata.empty:
            return {
                'Name': '1.2 In Tolerance',
                'Type': 'Condition',
                'Formula': f'$sample = $MB.toDiscrete().toCapsules().keep("Value",isBetween(105,95))\n'
                           f'$sampling_frequency.touches($sample)',
                'Formula Parameters': {'$MB': self.Mass_Balance(),
                                       '$sampling_frequency': self.Sampling()}
            }
        else:
            return {
                'Name': '1.2 In Tolerance',
                'Type': 'Condition',
                'Formula': f'$sample = $MB.toDiscrete().toCapsules().keep("Value",isBetween(103,97))\n'
                           f'$sampling_frequency.touches($sample)',
                'Formula Parameters': {'$MB': self.Mass_Balance(),
                                       '$sampling_frequency': self.Sampling()}
            }

    @Asset.Attribute()
    def Invalid(self, metadata):
        return {
            'Name': '1.3 Invalid',
            'Type': 'Condition',
            'Formula': f'$valid = $in or $out\n'
                       f'$sampling_frequency.subtract($valid)',
            'Formula Parameters': {'$in': self.In_Tolerance(),
                                   '$out': self.Out_of_Tolerance(),
                                   '$sampling_frequency': self.Sampling()}
        }

    @Asset.Attribute()
    def Shutdown(self, metadata):
        return {
            'Name': '1.4 Shutdown',
            'Type': 'Condition',
            'Formula': f'$invalid.touches($filtered)',
            'Formula Parameters': {'$filtered': self.Filtered(),
                                   '$invalid': self.Invalid()}
        }

    @Asset.Attribute()
    def Percent_Invalid(self, metadata):
        return {
            'Name': '2.3 Percentage Invalid',
            'Type': 'Signal',
            'Formula': '$invalid.aggregate(percentDuration(), $quarter, EndKey(),0s)',
            'Formula Parameters': {'$invalid': self.Invalid(),
                                   '$quarter': self.Quarter_Property()}
        }

    @Asset.Attribute()
    def Percent_Out_of_Tolerance(self, metadata):
        return {
            'Name': '2.1 Percentage Out of Tolerance',
            'Type': 'Signal',
            'Formula': '$out.aggregate(percentDuration(), $quarter, EndKey(),0s).move(7d)',
            'Formula Parameters': {'$out': self.Out_of_Tolerance(),
                                   '$quarter': self.Quarter_Property()}
        }

    @Asset.Attribute()
    def Percent_In_Tolerance(self, metadata):
        return {
            'Name': '2.2 Percentage In Tolerance',
            'Type': 'Signal',
            'Formula': '$in.aggregate(percentDuration(), $quarter, EndKey(),0s).move(14d)',
            'Formula Parameters': {'$in': self.In_Tolerance(),
                                   '$quarter': self.Quarter_Property()}
        }

    # -------------------------------------------------------------------------------------------------------------------
    @Asset.Attribute()
    def Combine_Signals(self, metadata):
        return {
            'Name': '3.1 Combine all percentages',
            'Type': 'Signal',
            'Formula': f'combineWith($a, $b, $c)',
            'Formula Parameters': {'$a': self.Percent_Invalid(),
                                   '$b': self.Percent_Out_of_Tolerance(),
                                   '$c': self.Percent_In_Tolerance()}
        }

    @Asset.Attribute()
    def Combine_Conditions(self, metadata):
        return {
            'Name': '3.2 Combine all conditions',
            'Type': 'Condition',
            'Formula': f'$invalid = $a.toCapsules().setProperty("Percent","1. Invalid")\n'
                       f'$out     = $b.toCapsules().setProperty("Percent","2. Out of Tolerance")\n'
                       f'$in      = $c.toCapsules().setProperty("Percent","3. In Tolerance")\n'
                       f'combineWith($invalid, $out, $in)',
            'Formula Parameters': {'$a': self.Percent_Invalid(),
                                   '$b': self.Percent_Out_of_Tolerance(),
                                   '$c': self.Percent_In_Tolerance()}
        }

    @Asset.Attribute()
    def Count_Out_of_Tolerance(self, metadata):
        return {
            'Name': '4.1 Count : Out of tolerance',
            'Type': 'Signal',
            'Formula': f'$out.aggregate(count(),$quarter, EndKey())',
            'Formula Parameters': {'$out': self.Out_of_Tolerance(),
                                   '$quarter': self.Quarter_Property()
                                   }
        }

    @Asset.Attribute()
    def Count_In_Tolerance(self, metadata):
        return {
            'Name': '4.2 Count : In tolerance',
            'Type': 'Signal',
            'Formula': f'$in.aggregate(count(),$quarter, EndKey())',
            'Formula Parameters': {'$in': self.In_Tolerance(),
                                   '$quarter': self.Quarter_Property()
                                   }
        }

    @Asset.Attribute()
    def Count_Mass_Balance(self, metadata):
        return {
            'Name': '4.3 Count : Mass Balance',
            'Type': 'Signal',
            'Formula': f'$count_in + $count_out',
            'Formula Parameters': {'$count_in': self.Count_In_Tolerance(),
                                   '$count_out': self.Count_Out_of_Tolerance()
                                   }
        }

    @Asset.Attribute()
    def Count_Invalid(self, metadata):
        return {
            'Name': '4.4 Count : Total Invalid',
            'Type': 'Signal',
            'Formula': f'$invalid.aggregate(count(),$quarter, EndKey())',
            'Formula Parameters': {'$invalid': self.Invalid(),
                                   '$quarter': self.Quarter_Property()
                                   }
        }

    @Asset.Attribute()
    def Count_Shutdown(self, metadata):
        return {
            'Name': '4.5 Count : Shutdown',
            'Type': 'Signal',
            'Formula': f'$Shutdown.aggregate(count(),$quarter, EndKey())',
            'Formula Parameters': {'$Shutdown': self.Shutdown(),
                                   '$quarter': self.Quarter_Property()
                                   }
        }

    @Asset.Attribute()
    def Count_Exclude(self, metadata):
        return {
            'Name': '4.6 Count : Exclude',
            'Type': 'Signal',
            'Formula': f'$count_invalid - $count_shutdown',
            'Formula Parameters': {'$count_shutdown': self.Count_Shutdown(),
                                   '$count_invalid': self.Count_Invalid()
                                   }
        }

    @Asset.Attribute()
    def Count_Max_Data_Points(self, metadata):
        return {
            'Name': '4.7 Count : Max data points from toolset',
            'Type': 'Signal',
            'Formula': f'$count_invalid + $count_mass',
            'Formula Parameters': {'$count_invalid': self.Count_Invalid(),
                                   '$count_mass': self.Count_Mass_Balance()
                                   }
        }

    @Asset.Attribute()
    def Biweeks_Exclude(self, metadata):
        return {
            'Name': '5.1 Biweeks Exclude',
            'Type': 'Signal',
            'Formula': f'($count_shutdown / $count_max)*6',
            'Formula Parameters': {'$count_shutdown': self.Count_Shutdown(),
                                   '$count_max': self.Count_Max_Data_Points()
                                   }
        }

    @Asset.Attribute()
    def Biweeks_Possible(self, metadata):
        return {
            'Name': '5.2 Biweeks Possible',
            'Type': 'Signal',
            'Formula': f'6 - $Biweeks_exclude',
            'Formula Parameters': {'$Biweeks_exclude': self.Biweeks_Exclude()}
        }

    @Asset.Attribute()
    def Biweeks_wMB_Data(self, metadata):
        return {
            'Name': '5.3 Biweeks w/MB Data',
            'Type': 'Signal',
            'Formula': f'$biweeks_possible - (($count_exclude/$count_max)*6)',
            'Formula Parameters': {'$biweeks_possible': self.Biweeks_Possible(),
                                   '$count_max': self.Count_Max_Data_Points(),
                                   '$count_exclude': self.Count_Exclude()
                                   }
        }

    @Asset.Attribute()
    def Biweeks_wMB_Data_in_Tolerance(self, metadata):
        return {
            'Name': '5.4 Biweeks w/MB Data in Tolerance',
            'Type': 'Signal',
            'Formula': f'($count_in / $count_max)*6',
            'Formula Parameters': {'$count_in': self.Count_In_Tolerance(),
                                   '$count_max': self.Count_Max_Data_Points()
                                   }
        }

    @Asset.Attribute()
    def kEdC_Biweeks_Possible(self, metadata):
        return {
            'Name': '6.1 kEdC*Biweeks Possible',
            'Type': 'Signal',
            'Formula': f'$kEDC * $biweeks_possible',
            'Formula Parameters': {'$kEDC': self.kEDC(),
                                   '$biweeks_possible': self.Biweeks_Possible()
                                   }
        }

    @Asset.Attribute()
    def kEDC_Biweeks_wMB(self, metadata):
        return {
            'Name': '6.2 kEDC*Biweeks w/MB',
            'Type': 'Signal',
            'Formula': f'$kEDC * $Biweeks_wMB_Data',
            'Formula Parameters': {'$kEDC': self.kEDC(),
                                   '$Biweeks_wMB_Data': self.Biweeks_wMB_Data()
                                   }
        }

    @Asset.Attribute()
    def kEDC_Biweeks_wMB_in_Tol(self, metadata):
        return {
            'Name': '6.3 kEDC*Biweeks w/MB in Tolerance',
            'Type': 'Signal',
            'Formula': f'$kEDC * $Biweeks_wMB_Data_in_Tolerance',
            'Formula Parameters': {'$kEDC': self.kEDC(),
                                   '$Biweeks_wMB_Data_in_Tolerance': self.Biweeks_wMB_Data_in_Tolerance()
                                   }
        }

    @Asset.Attribute()
    def Metric_1(self, metadata):
        return {
            'Name': '7.2 Metric 1',
            'Type': 'Signal',
            'Formula': f'(($kEDC_Biweeks_wMB - $kEDC_Biweeks_wMB_in_Tol)/$total_site_kEDC).round(5)',
            'Formula Parameters': {'$kEDC_Biweeks_wMB': self.kEDC_Biweeks_wMB(),
                                   '$kEDC_Biweeks_wMB_in_Tol': self.kEDC_Biweeks_wMB_in_Tol(),
                                   '$total_site_kEDC': self.parent.Total_Site_kEDC(metadata)
                                   }
        }


@pytest.mark.unit
def test_crab_32080():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'EMEAP Master Asset Tree Structure_ver2.0.csv'))

    df['Build Asset'] = '*Prototype Mass Balance'
    df['Build Path'] = None

    build_metadata = spy.assets.build(Prototype_Mass_Balance, df, errors='raise').dropna(subset=['Type'])

    assert len(build_metadata) > 1000
