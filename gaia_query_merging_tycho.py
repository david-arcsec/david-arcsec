#!/usr/bin/env python
# coding: utf-8

# # This program queries edr3gaia & tycho2 for data, and makes a csv file with their relevant information

# ### Have the tycho2 database (with mag < 7), and all of those stars that are also in the edr3. Inclued the variables: RA_tycho2, Dec_tycho2, RA_edr3, Dec_edr3, proper motion, color/spectra (pick the band that is brightest in the visible range: 442 nm for Bt, 540 𝑛𝑚 𝑓𝑜𝑟 𝑉𝑡 )

# In[62]:


from astroquery.gaia import Gaia
import csv
import pandas as pd


# In[52]:


# Write a query for the brightest stars in Tycho2 and GaiaEDR3
job = Gaia.launch_job_async("SELECT public.tycho2.id, gaiaedr3.tycho2tdsc_merge_best_neighbour.original_ext_source_id, gaiaedr3.tycho2tdsc_merge_best_neighbour.source_id, gaiaedr3.gaia_source.source_id, public.tycho2.ra, public.tycho2.dec, gaiaedr3.gaia_source.ra, gaiaedr3.gaia_source.dec, public.tycho2.vt_mag, gaiaedr3.gaia_source.pseudocolour, gaiaedr3.gaia_source.phot_g_mean_mag, gaiaedr3.gaia_source.pm, gaiaedr3.gaia_source.pmra, gaiaedr3.gaia_source.pmdec FROM public.tycho2, gaiaedr3.tycho2tdsc_merge_best_neighbour, gaiaedr3.gaia_source WHERE (public.tycho2.vt_mag<=7) AND (public.tycho2.id=gaiaedr3.tycho2tdsc_merge_best_neighbour.original_ext_source_id) AND (gaiaedr3.tycho2tdsc_merge_best_neighbour.source_id=gaiaedr3.gaia_source.source_id) ORDER by public.tycho2.vt_mag;", dump_to_file=False)

survey = job.get_results()


# In[ ]:


path = '/home/dcpetit/Documents/work_employment/arcsec/Gaia_Investigations/'
file = 'tycho2_gaia_bright_database.csv'
f = open(path+file, 'w')
writer = csv.writer(f)
writer.writerow(['id', 'original_ext_source_id', 'source_id_TycMergeGaia', 'source_id_gaia', 'ra_tyc', 'dec_tyc', 'ra_gaia', 'dec_gaia', 'vt_mag', 'pseudocolour', 'phot_g_mean_mag', 'pm [mas/yr]', 'pmra', 'pmdec']) # Write the column headers ###
writer.writerows(survey)
f.close()
dataFrame = pd.read_csv(path+file)


# In[75]:


dataFrame['ra_error'] = dataFrame['ra_gaia'] - dataFrame['ra_tyc']
dataFrame['ra_difference'] = abs( dataFrame['ra_gaia'] - dataFrame['ra_tyc'] )
dataFrame['dec_error'] = dataFrame['dec_gaia'] - dataFrame['dec_tyc']
dataFrame['dec_difference'] = abs( dataFrame['dec_gaia'] - dataFrame['dec_tyc'] )
print('The sum of the ra errors (~0):     ', round(dataFrame['ra_error'].sum(),3))
print('The sum of the ra differnces (>0): ', round(dataFrame['ra_difference'].sum(),3))
print('The sum of the dec errors (~0):   ', round(dataFrame['dec_error'].sum(),3))
print('The sum of the dec differnces (>0):', round(dataFrame['dec_difference'].sum(),3))
print('The average of the ra errors (~0):     ', round(dataFrame['ra_error'].mean(),6))
print('The average of the ra differnces (>0): ', round(dataFrame['ra_difference'].mean(),6))
print('The average of the dec errors (~0):   ', round(dataFrame['dec_error'].mean(),6))
print('The average of the dec differnces (>0):', round(dataFrame['dec_difference'].mean(),6))


# In[60]:


dataFrame.to_csv(path+file)


# In[ ]:


''' The Code appendix (trash)
If headers were automatically populated, and then needed renaming:
#dataFrame['ra_tyc'] = dataFrame['']
#dataFrame['ra_gaia'] = dataFrame[:,5]
#dataFrame['dec_tyc'] = dataFrame[:,6]
#dataFrame['dec_gaia'] = dataFrame[:,7]
#dataFrame.drop(['ra', 'ra_2', 'dec', 'dec_2'])

If printing a dataFrame, and few decimal places are visibly wanted
pd.options.display.float_format = "{:,.2f}".format
'''

