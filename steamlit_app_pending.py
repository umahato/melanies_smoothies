# Import python packages
import streamlit as st
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched


# Write directly to the app
st.title(f"Pending Smoothie Orders :cup_with_straw: ")
st.write(
  """Orders that need to be filled.
  """
)

#option = st.selectbox(
#    "What is your favorite fruit?",
#    ("Banana", "Strawberries", "Peaches"),
#)

#st.write("Your favourite fruit is :", option)

name_on_order=st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:' , name_on_order)

cnx = st.connection("snowflake")
#session = get_active_session()
session = cnx.session()

#my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

#editable_df = st.data_editor(my_dataframe)
#submitted = st.button('Submit')

# Submit button logic
#if submitted:
#    for index, row in editable_df.iterrows():
#        name_on_order = row['NAME_ON_ORDER']  # Use the correct column name
#        order_filled = row['ORDER_FILLED']
#
#        if order_filled == 1:  # Only update if marked as filled
#            # Prepare the SQL UPDATE query
#            my_update_stmt = f"""
#                UPDATE smoothies.public.orders
#                SET ORDER_FILLED = 1
#                WHERE NAME_ON_ORDER = '{name_on_order}'
#            """
#
#            # Execute the update query
#            session.sql(my_update_stmt).collect()

#    st.success("Someone clicked the button! 👍")

if my_dataframe:
    editable_df=st.data_editor(my_dataframe)
    submitted = st.button('Submit')
    if submitted:
        og_dataset=session.table("smoothies.public.orders")
        edited_dataset=session.create_dataframe(editable_df)

        try:
            og_dataset.merge(
                edited_dataset,
                      og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']
                     , [when_matched().update({
                         'ORDER_FILLED': edited_dataset['ORDER_FILLED']
                     })
                       ]
                    )
            st.success("Order(s) Updated!" ,icon="👍")
        except:
            st.write('Something went wrong.')


else:
    st.success('There are no pending orders right now' ,icon="👍")
    

   
