### Extended meta data section v4.0

The extended meta data section will be used to send extra meta data for each mutation, deletion or expiration. This section comes at the very end of the message right after the value section. The nmeta field in the packet will contain the length of this field.

#### Version 1 (0x01)

In this version, the extended meta data section will have the following format:

    | version | id_1 | len_1 | field_1 | ... | id_n | len_n | field_n |

Here:
* version: is of length 1B, and will contain 0x01.
* id_n: is the id indicating what will be in the field, is of length 1B.
* len_n: is of length 2B, and will contain the length of the following field.
* field_n: is of length "len_n"B, and will contain the value for the particular type.

**Meta Data IDs:**

* 0x01 - adjusted time
* 0x02 - conflict resolution mode

#### Operation

* For the extended meta data to be carried in a message (mutation/deletion/expiration), a control message will need to be sent for key "enable_ext_metadata" by the consumer to the producer after making the connection.
* The value for enable_ext_metadata needs to be set to true.

* Once the control message is sent, if time_synchronization is enabled (can be toggled through the set_drift_counter_state API), the adjusted time will be sent as part of the mutation/deletion/expiration message. The conflict resolution mode will be included always in a mutation, deletion or expiration, if the control message for enable_ext_metadata:true was sent by the consumer.
