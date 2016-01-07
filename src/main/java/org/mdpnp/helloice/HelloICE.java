package org.mdpnp.helloice;

import com.rti.dds.domain.DomainParticipantQos;
import com.rti.dds.publication.Publisher;
import com.rti.dds.publication.PublisherQos;
import com.rti.dds.subscription.*;
import ice.NumericDataReader;
import ice.SampleArrayDataReader;
import ice.Time_t;

import org.mdpnp.rtiapi.data.QosProfiles;
import org.mdpnp.rtiapi.qos.IceQos;

import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.infrastructure.ConditionSeq;
import com.rti.dds.infrastructure.Duration_t;
import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.RETCODE_NO_DATA;
import com.rti.dds.infrastructure.ResourceLimitsQosPolicy;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.infrastructure.WaitSet;
import com.rti.dds.topic.Topic;

public class HelloICE {
    public static void receiveOnMiddlewareThread(final DomainParticipant participant, final Topic sampleArrayTopic, final Topic numericTopic) {
        
        // A listener to receive callback events from the SampleArrayDataReader
        final DataReaderListener saListener = new DataReaderListener() {

            @Override
            public void on_data_available(DataReader reader) {
                // Will contain the data samples we read from the reader
                ice.SampleArraySeq sa_data_seq = new ice.SampleArraySeq();

                // Will contain the SampleInfo information about those data
                SampleInfoSeq info_seq = new SampleInfoSeq();

                SampleArrayDataReader saReader = (SampleArrayDataReader) reader;

                // Read samples from the reader
                try {
                    saReader.read(sa_data_seq,info_seq, ResourceLimitsQosPolicy.LENGTH_UNLIMITED, SampleStateKind.NOT_READ_SAMPLE_STATE, ViewStateKind.ANY_VIEW_STATE, InstanceStateKind.ALIVE_INSTANCE_STATE);

                    // Iterator over the samples
                    for(int i = 0; i < info_seq.size(); i++) {
                        SampleInfo si = (SampleInfo) info_seq.get(i);
                        ice.SampleArray data = (ice.SampleArray) sa_data_seq.get(i);
                        // If the updated sample status contains fresh data that we can evaluate
                        if(si.valid_data) {
                            System.out.println(data);
                        }

                    }
                } catch (RETCODE_NO_DATA noData) {
                    // No Data was available to the read call
                } finally {
                    // the objects provided by "read" are owned by the reader and we must return them
                    // so the reader can control their lifecycle
                    saReader.return_loan(sa_data_seq, info_seq);
                }
            }

            @Override
            public void on_liveliness_changed(DataReader arg0, LivelinessChangedStatus arg1) {
                System.out.println("liveliness_changed "+arg1);
            }

            @Override
            public void on_requested_deadline_missed(DataReader arg0, RequestedDeadlineMissedStatus arg1) {
                System.out.println("requested_deadline_missed "+arg1);
            }

            @Override
            public void on_requested_incompatible_qos(DataReader arg0, RequestedIncompatibleQosStatus arg1) {
                System.out.println("requested_incompatible_qos "+arg1);
            }

            @Override
            public void on_sample_lost(DataReader arg0, SampleLostStatus arg1) {
                System.out.println("sample_lost "+arg1);
            }

            @Override
            public void on_sample_rejected(DataReader arg0, SampleRejectedStatus arg1) {
                System.out.println("sample_rejected "+arg1);
            }

            @Override
            public void on_subscription_matched(DataReader arg0, SubscriptionMatchedStatus arg1) {
                System.out.println("subscription_matched "+arg1);
            }
            
        };
        
        // A listener to receive callback events from the NumericDataReader
        final DataReaderListener nListener = new DataReaderListener() {
            @Override
            public void on_data_available(DataReader reader) {
                ice.NumericSeq n_data_seq = new ice.NumericSeq();

                // Will contain the SampleInfo information about those data
                SampleInfoSeq info_seq = new SampleInfoSeq();
                
                NumericDataReader nReader = (NumericDataReader) reader;
                
                try {
                    // Read samples from the reader
                    nReader.read(n_data_seq,info_seq, ResourceLimitsQosPolicy.LENGTH_UNLIMITED, SampleStateKind.NOT_READ_SAMPLE_STATE, ViewStateKind.ANY_VIEW_STATE, InstanceStateKind.ALIVE_INSTANCE_STATE);

                    // Iterator over the samples
                    for(int i = 0; i < info_seq.size(); i++) {
                        SampleInfo si = (SampleInfo) info_seq.get(i);
                        ice.Numeric data = (ice.Numeric) n_data_seq.get(i);
                        // If the updated sample status contains fresh data that we can evaluate
                        if(si.valid_data) {
                            if(data.metric_id.equals(rosetta.MDC_PULS_OXIM_SAT_O2.VALUE)) {
                                // This is an O2 saturation from pulse oximetry
//                                System.out.println("SpO2="+data.value);
                            } else if(data.metric_id.equals(rosetta.MDC_PULS_OXIM_PULS_RATE.VALUE)) {
                                // This is a pulse rate from pulse oximetry
//                              System.out.println("Pulse Rate="+data.value);
                            }
                            System.out.println(data);
                        }

                    }
                } catch (RETCODE_NO_DATA noData) {
                    // No Data was available to the read call
                } finally {
                    // the objects provided by "read" are owned by the reader and we must return them
                    // so the reader can control their lifecycle
                    nReader.return_loan(n_data_seq, info_seq);
                }

            }
            
            @Override
            public void on_liveliness_changed(DataReader arg0, LivelinessChangedStatus arg1) {
                System.out.println("liveliness_changed "+arg1);
            }

            @Override
            public void on_requested_deadline_missed(DataReader arg0, RequestedDeadlineMissedStatus arg1) {
                System.out.println("requested_deadline_missed "+arg1);
            }

            @Override
            public void on_requested_incompatible_qos(DataReader arg0, RequestedIncompatibleQosStatus arg1) {
                System.out.println("requested_incompatible_qos "+arg1);
            }

            @Override
            public void on_sample_lost(DataReader arg0, SampleLostStatus arg1) {
                System.out.println("sample_lost "+arg1);
            }

            @Override
            public void on_sample_rejected(DataReader arg0, SampleRejectedStatus arg1) {
                System.out.println("sample_rejected "+arg1);
            }

            @Override
            public void on_subscription_matched(DataReader arg0, SubscriptionMatchedStatus arg1) {
                System.out.println("subscription_matched "+arg1);
            }
        };
        
        // Create a reader endpoint for samplearray data
        @SuppressWarnings("unused")
        ice.SampleArrayDataReader saReader = (ice.SampleArrayDataReader) participant.create_datareader_with_profile(sampleArrayTopic, QosProfiles.ice_library, QosProfiles.waveform_data, saListener, StatusKind.STATUS_MASK_ALL);

        @SuppressWarnings("unused")
        ice.NumericDataReader nReader = (ice.NumericDataReader) participant.create_datareader_with_profile(numericTopic, QosProfiles.ice_library, QosProfiles.numeric_data, nListener, StatusKind.STATUS_MASK_ALL);

    }

    
    public static void receiveOnMyThreadByConditionVar(final DomainParticipant participant, final Topic sampleArrayTopic, final Topic numericTopic) {
        // Create a reader endpoint for samplearray data
        ice.SampleArrayDataReader saReader = (ice.SampleArrayDataReader) participant.create_datareader_with_profile(sampleArrayTopic, QosProfiles.ice_library, QosProfiles.waveform_data, null, StatusKind.STATUS_MASK_NONE);

        ice.NumericDataReader nReader = (ice.NumericDataReader) participant.create_datareader_with_profile(numericTopic, QosProfiles.ice_library, QosProfiles.numeric_data, null, StatusKind.STATUS_MASK_NONE);

        // A waitset allows us to wait for various status changes in various entities
        WaitSet ws = new WaitSet();

        // Here we configure the status condition to trigger when new data becomes available to the reader
        saReader.get_statuscondition().set_enabled_statuses(StatusKind.DATA_AVAILABLE_STATUS);

        nReader.get_statuscondition().set_enabled_statuses(StatusKind.DATA_AVAILABLE_STATUS);

        // And register that status condition with the waitset so we can monitor its triggering
        ws.attach_condition(saReader.get_statuscondition());

        ws.attach_condition(nReader.get_statuscondition());

        // will contain triggered conditions
        ConditionSeq cond_seq = new ConditionSeq();

        // we'll wait as long as necessary for data to become available
        Duration_t timeout = new Duration_t(Duration_t.DURATION_INFINITE_SEC, Duration_t.DURATION_INFINITE_NSEC);

        // Will contain the data samples we read from the reader
        ice.SampleArraySeq sa_data_seq = new ice.SampleArraySeq();

        ice.NumericSeq n_data_seq = new ice.NumericSeq();

        // Will contain the SampleInfo information about those data
        SampleInfoSeq info_seq = new SampleInfoSeq();

        // This loop will repeat until the process is terminated
        for(;;) {
            // Wait for a condition to be triggered
            ws.wait(cond_seq, timeout);
            // Check that our status condition was indeed triggered
            if(cond_seq.contains(saReader.get_statuscondition())) {
                // read the actual status changes
                int status_changes = saReader.get_status_changes();

                // Ensure that DATA_AVAILABLE is one of the statuses that changed in the DataReader.
                // Since this is the only enabled status (see above) this is here mainly for completeness
                if(0 != (status_changes & StatusKind.DATA_AVAILABLE_STATUS)) {
                    try {
                        // Read samples from the reader
                        saReader.read(sa_data_seq,info_seq, ResourceLimitsQosPolicy.LENGTH_UNLIMITED, SampleStateKind.NOT_READ_SAMPLE_STATE, ViewStateKind.ANY_VIEW_STATE, InstanceStateKind.ALIVE_INSTANCE_STATE);

                        // Iterator over the samples
                        for(int i = 0; i < info_seq.size(); i++) {
                            SampleInfo si = (SampleInfo) info_seq.get(i);
                            ice.SampleArray data = (ice.SampleArray) sa_data_seq.get(i);
                            // If the updated sample status contains fresh data that we can evaluate
                            if(si.valid_data) {
                                System.out.println(data);
                            }

                        }
                    } catch (RETCODE_NO_DATA noData) {
                        // No Data was available to the read call
                    } finally {
                        // the objects provided by "read" are owned by the reader and we must return them
                        // so the reader can control their lifecycle
                        saReader.return_loan(sa_data_seq, info_seq);
                    }
                }
            }
            if(cond_seq.contains(nReader.get_statuscondition())) {
                // read the actual status changes
                int status_changes = nReader.get_status_changes();

                // Ensure that DATA_AVAILABLE is one of the statuses that changed in the DataReader.
                // Since this is the only enabled status (see above) this is here mainly for completeness
                if(0 != (status_changes & StatusKind.DATA_AVAILABLE_STATUS)) {
                    try {
                        // Read samples from the reader
                        nReader.read(n_data_seq,info_seq, ResourceLimitsQosPolicy.LENGTH_UNLIMITED, SampleStateKind.NOT_READ_SAMPLE_STATE, ViewStateKind.ANY_VIEW_STATE, InstanceStateKind.ALIVE_INSTANCE_STATE);

                        // Iterator over the samples
                        for(int i = 0; i < info_seq.size(); i++) {
                            SampleInfo si = (SampleInfo) info_seq.get(i);
                            ice.Numeric data = (ice.Numeric) n_data_seq.get(i);
                            // If the updated sample status contains fresh data that we can evaluate
                            if(si.valid_data) {
                                if(data.metric_id.equals(rosetta.MDC_PULS_OXIM_SAT_O2.VALUE)) {
                                    // This is an O2 saturation from pulse oximetry
//                                    System.out.println("SpO2="+data.value);
                                } else if(data.metric_id.equals(rosetta.MDC_PULS_OXIM_PULS_RATE.VALUE)) {
                                    // This is a pulse rate from pulse oximetry
//                                  System.out.println("Pulse Rate="+data.value);
                                }
                                System.out.println(data);
                            }

                        }
                    } catch (RETCODE_NO_DATA noData) {
                        // No Data was available to the read call
                    } finally {
                        // the objects provided by "read" are owned by the reader and we must return them
                        // so the reader can control their lifecycle
                        nReader.return_loan(n_data_seq, info_seq);
                    }
                }
            }
        }
    }
    
    
    public static void sendOnThisThread(DomainParticipant participant, Topic sampleArrayTopic, Topic numericTopic) throws InterruptedException {
        // Creates a data writer; uses the default implicit publisher for this participant
        ice.SampleArrayDataWriter saWriter = (ice.SampleArrayDataWriter) participant.create_datawriter_with_profile(sampleArrayTopic, QosProfiles.ice_library, QosProfiles.waveform_data, null, StatusKind.STATUS_MASK_NONE);

        ice.NumericDataWriter nWriter = (ice.NumericDataWriter) participant.create_datawriter_with_profile(numericTopic, QosProfiles.ice_library, QosProfiles.numeric_data, null, StatusKind.STATUS_MASK_NONE);
        
        // Populate the values of a sample to be written later
        ice.Numeric numeric = new ice.Numeric();
        numeric.unique_device_identifier = "1111";
        numeric.metric_id = rosetta.MDC_ECG_HEART_RATE.VALUE;
        numeric.unit_id = rosetta.MDC_DIM_BEAT_PER_MIN.VALUE;
        numeric.vendor_metric_id = "";
        numeric.instance_id = 0;
        numeric.device_time = new Time_t();
        numeric.presentation_time = new Time_t();
        
        ice.SampleArray sampleArray = new ice.SampleArray();
        sampleArray.unique_device_identifier = "1111";
        sampleArray.metric_id = ice.MDC_ECG_LEAD_I.VALUE;
        sampleArray.unit_id = rosetta.MDC_DIM_MILLI_VOLT.VALUE;
        sampleArray.vendor_metric_id = "";
        sampleArray.instance_id = 0;
        sampleArray.device_time = new Time_t();
        sampleArray.presentation_time = new Time_t();
        sampleArray.frequency = 10;
        
        // Preregistering instances speeds up subsequent writes 
        InstanceHandle_t saHandle = saWriter.register_instance(sampleArray);
        InstanceHandle_t nHandle = nWriter.register_instance(numeric);
        
        // Write 
        for(int i = 0; i < 10; i++) {
            long time = System.currentTimeMillis();
            
            numeric.device_time.sec = (int) (time / 1000L);
            numeric.device_time.nanosec = (int) (time % 1000L * 1000000L);
            numeric.presentation_time.copy_from(numeric.device_time);
            nWriter.write(numeric, nHandle);
            
            sampleArray.values.clear();
            
            sampleArray.device_time.copy_from(numeric.device_time);
            sampleArray.presentation_time.copy_from(numeric.presentation_time);
            
            // Square wave
            if(0 == (i%2)) {
                for(int j = 0; j < 10; j++) {
                    sampleArray.values.userData.add(0.0f);
                }
            } else {
                for(int j = 0; j < 10; j++) {
                    sampleArray.values.userData.add(1.0f);
                }
                
            }
            saWriter.write(sampleArray, saHandle);
            
            System.out.println("Wrote " + numeric);
            System.out.println(sampleArray);
            
            Thread.sleep(1000L);
        }
        
        saWriter.unregister_instance(sampleArray, saHandle);
        nWriter.unregister_instance(numeric, nHandle);
        
    }
    
    enum ReceiveStrategy {
        OnMyThreadByConditionVar,
        OnMiddlewareThread,
        PublishExample,
    }
    
    public static void main(String[] args) throws InterruptedException {
        int domainId = 0;

        // domainId is the one command line argument
        if(args.length > 0) {
            System.out.println("Using Domain " + args[0]);
            domainId = Integer.parseInt(args[0]);
        }

        // Here we use 'default' Quality of Service settings supplied by x73-idl-rti-dds
        IceQos.loadAndSetIceQos();

        // A domain participant is the main access point into the DDS domain.  Endpoints are created within the domain participant
        DomainParticipant participant = DomainParticipantFactory.get_instance().create_participant(domainId, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);



        // OpenICE divides the global DDS data-space into individual patient contexts using the DDS partitioning mechanism.
        // Partitions are a list of strings (and wildcards) that pubs and subs are required to match (at least one once) in their respective lists to pair.
        // Partitions are assigned at the publisher/subscriber level in the quality of service (QoS) settings.

        // Declare and instantiate a new SubscriberQos policy
        SubscriberQos subscriberQos = new SubscriberQos();

        // Populate the SubscriberQos policy
        participant.get_default_subscriber_qos(subscriberQos);

        subscriberQos.partition.name.clear();

        // Add an entry to the partition list. 'name' is actually a DDS StringSeq that you can simply .add() to.
        // To receive OpenICE data for a specific patient, add the patient's MRN to the QoS partition policy
        // e.g. "MRN=12345". MRNs are alphanumeric prefixed with "MRN="
        // subscriberQos.partition.name.add("MRN=10101");   // This is fake patient Randall Jones in the MDPnP lab.

        // Partitioning supports some wildcards like "*" to access every partition (including the default or null partition)
        subscriberQos.partition.name.add("*");

        // A note about partition names:
        // The Supervisor uses an MRN for the Partition name and a First / Last name as a display name. For example, MRN=10101
        // will show up as Randall Jones in the Supervisor. To match display names and MRNs, the Supervisor will either
        // use defaults or look them up in an HL7 FHIR database. If you provide an HL7 FHIR server address, the Supervisor
        // will treat that server as a "Master Patient Index". The Supervisor will attempt to download the Patient Resource
        // (https://www.hl7.org/fhir/patient.html) from that address and use resource.identifier.value as the Partition name and
        // resource.name.family / resource.name.given as the display name. If no HL7 FHIR address is provided, the Supervisor will
        // provide a small SQL server of default names and MRNs.

        // Set the subscriber qos with our newly created SubscriberQos
        participant.set_default_subscriber_qos(subscriberQos);

        // There are a couple ways to do this (as far as I can tell). I don't know which one is correct or standard or proper or whatever. For example:
        // Subscriber subscriber = participant.get_implicit_subscriber();
        // subscriber.get_qos(subscriberQos);
        // subscriberQos.partition.name.add("MRN=10101");
        // subscriber.set_qos(subscriberQos);


        // Same concept but this time for the publisher
        PublisherQos publisherQos = new PublisherQos();

        participant.get_default_publisher_qos(publisherQos);

        publisherQos.partition.name.clear();

        // Change this line to the patient MRN for which you want to emit data.
//        publisherQos.partition.name.add("MRN=10101");

        participant.set_default_publisher_qos(publisherQos);



        // Inform the participant about the sample array data type we would like to use in our endpoints
        ice.SampleArrayTypeSupport.register_type(participant, ice.SampleArrayTypeSupport.get_type_name());

        // Inform the participant about the numeric data type we would like to use in our endpoints
        ice.NumericTypeSupport.register_type(participant, ice.NumericTypeSupport.get_type_name());

        // A topic the mechanism by which reader and writer endpoints are matched.
        Topic sampleArrayTopic = participant.create_topic(ice.SampleArrayTopic.VALUE, ice.SampleArrayTypeSupport.get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        // A second topic if for Numeric data
        Topic numericTopic = participant.create_topic(ice.NumericTopic.VALUE, ice.NumericTypeSupport.get_type_name(), DomainParticipant.TOPIC_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

        ReceiveStrategy strategy = ReceiveStrategy.OnMiddlewareThread;
        
        if(args.length > 1) {
            strategy = ReceiveStrategy.valueOf(args[1]);
        }

        System.out.println("strategy: " + strategy);

        switch(strategy) {
        // receiveOnMyThreadByConditionVar demonstrates receiving data on *this* thread via notification by condition variable.
        // Alternatively in unique cases readers can be polled at intervals with no signalling.
        case OnMyThreadByConditionVar:
            receiveOnMyThreadByConditionVar(participant, sampleArrayTopic, numericTopic);
            break;
        // receiveOnMiddlewareThread demonstrates receiving data via a callback on a middleware thread.
        case OnMiddlewareThread:
            receiveOnMiddlewareThread(participant, sampleArrayTopic, numericTopic);
            break;
        case PublishExample:
            sendOnThisThread(participant, sampleArrayTopic, numericTopic);
            break;
        }
        
    }
}
