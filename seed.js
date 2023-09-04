const fs = require('fs');
const fsExtra = require('fs-extra');
const { DownloaderHelper } = require('node-downloader-helper');
const config = require('./config.json');
const decompress = require('decompress');
const path = require('path');
const replace = require('replace-in-file');
const { Pool } = require('pg')

Number.prototype.pad = function (size) {
    var s = String(this);
    while (s.length < (size || 2)) { s = "0" + s; }
    return s;
}

const downloadFiles = async () => {
    try {
        if (!fs.existsSync(config.downloadPath)) {
            await fs.mkdir(config.downloadPath);
        }
        await fsExtra.emptyDir(config.downloadPath);
        if (!fs.existsSync(config.extractPath)) {
            await fs.mkdir(config.extractPath);
        }
        await fsExtra.emptyDir(config.extractPath);

        const tvDl = new DownloaderHelper(config.tvUrl, config.downloadPath);
        await tvDl.start();
        console.log("TV Data Download Complete");

        var curDate = `${(new Date().getMonth() + 1).pad()}-${new Date().getDate().pad()}-${new Date().getFullYear()}`;

        const amDl = new DownloaderHelper(config.amUrl.replace("{{DATE}}", curDate), config.downloadPath);
        await amDl.start();
        console.log('AM Data Download Complete');

        const fmDl = new DownloaderHelper(config.fmUrl.replace("{{DATE}}", curDate), config.downloadPath);
        await fmDl.start();
        console.log('FM Data Download Complete');

        for (let i = 0; i < config.ulsDataTypes.length; i++) {
            const ulsRegDl = new DownloaderHelper(config.ulsRegUrl.replace("{{DATA_TYPE}}", config.ulsDataTypes[i]), config.downloadPath);
            await ulsRegDl.start();
            const ulsLicDl = new DownloaderHelper(config.ulsLicUrl.replace("{{DATA_TYPE}}", config.ulsDataTypes[i]), config.downloadPath);
            await ulsLicDl.start();
            console.log(`ULS ${config.ulsDataTypes[i]} Data Download Complete`);
        }

        const asrDl = new DownloaderHelper(config.asrLicensesUrl, config.downloadPath);
        await asrDl.start();
        console.log('ASR Data Download Complete');
    } catch (e) {
        console.log(e);
    }
}

const unzipFiles = async () => {
    if (!fs.existsSync(config.extractPath)) {
        await fs.mkdir(config.extractPath);
    }

    const zipFiles = (await fs.promises.readdir(config.downloadPath)).filter(file => path.extname(file) === '.zip');

    for (let i = 0; i < zipFiles.length; i++) {
        if (config.ulsDataTypes.some(dataType => zipFiles[i].includes(dataType))) {
            await decompress(config.downloadPath + "/" + zipFiles[i], config.extractPath, { filter: file => file.path == 'LO.dat', map: file => { file.path = zipFiles[i].substring(0, zipFiles[i].length - 4) + "-" + file.path; return file; } });
        } else if (zipFiles[i] == "r_tower.zip") {
            await decompress(config.downloadPath + "/" + zipFiles[i], config.extractPath, { filter: file => file.path == 'CO.dat' || file.path == 'RA.dat', map: file => { file.path = zipFiles[i].substring(0, zipFiles[i].length - 4) + "-" + file.path; return file; } });
        } else {
            await decompress(config.downloadPath + "/" + zipFiles[i], config.extractPath);
        }
        console.log("Unzipped " + zipFiles[i]);
    }
}

const cleanFiles = async () => {
    await replace({ files: `${config.extractPath}/*.dat`, from: /\\\|/g, to: '|' });

    console.log("Cleaned .dat files");

    await replace({ files: [`${config.extractPath}/gis_am_ant_sys.dat`, `${config.extractPath}/gis_fm_eng_data.dat`, `${config.extractPath}/tv_eng_data.dat`], from: /\|\^\|/g, to: '' });

    console.log("Cleaned eng_dat files");
}

const createTables = async () => {
    //INSTALL POSTGIS 4.2.2 TO POSTGRES BEFORE THIS STEP

    const pool = new Pool({
        host: config.pgHost,
        port: config.pgPort,
        user: config.pgUsername,
        password: config.pgPassword,
        database: config.pgDatabase,
        max: 20,
        keepAlive: true,
    });

    const postgisExt = await pool.query(`CREATE EXTENSION IF NOT EXISTS postgis;`)

    console.log("Installed Postgis");

    const isNumericFunc = await pool.query(`
        CREATE OR REPLACE FUNCTION isnumeric(text) RETURNS BOOLEAN AS $$
        DECLARE x NUMERIC;
        BEGIN
            x = $1::NUMERIC;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
            RETURN FALSE;
        END;
        $$
        STRICT
        LANGUAGE plpgsql IMMUTABLE;
    `);

    const transformSafeFunc = await pool.query(`
        CREATE OR REPLACE FUNCTION transform_safe(geom geometry, srid int) RETURNS geometry AS $$
        BEGIN
        IF ST_Srid(geom) = 0 THEN
            RAISE Exception 'Input geometry has unknown (0) SRID';
        END IF;
        BEGIN
            RETURN ST_Transform(geom, srid);
        EXCEPTION WHEN internal_error THEN
            RAISE WARNING '%: %',SQLSTATE,SQLERRM;
        END;
        RETURN NULL;
        END;

        $$
        language plpgsql;
    `);

    const dms2ddFunc = await pool.query(`
        CREATE OR REPLACE FUNCTION public.dms2dd(
            d numeric,
            m numeric,
            s numeric,
            hemi character varying)
            RETURNS double precision
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            AS $BODY$
        DECLARE
        ret double precision;
        dir integer;
        BEGIN
        dir := 1;
        ret := 0;
        IF UPPER(HEMI) = 'S' OR UPPER(HEMI) = 'W' THEN
        dir := -1;
        END IF;
        ret := (ABS(CAST(D as double precision)) + (ABS((CAST(M as double precision) + (ABS((CAST(S as double precision))/60)))/60)));
        ret := ret * dir;
        RETURN ret;

        END;
        $BODY$;
    `);

    console.log("Created data functions");

    const asrLocationsTable = await pool.query(`
        CREATE TABLE IF NOT EXISTS public.asr_locations
        (
            record_type text COLLATE pg_catalog."default",
            content_indicator text COLLATE pg_catalog."default",
            file_number text COLLATE pg_catalog."default",
            registration_number text COLLATE pg_catalog."default",
            unique_system_identifier text NOT NULL,
            coordinate_type text COLLATE pg_catalog."default" NOT NULL,
            latitude_degrees text COLLATE pg_catalog."default",
            latitude_minutes text COLLATE pg_catalog."default",
            latitude_seconds text COLLATE pg_catalog."default",
            latitude_direction text COLLATE pg_catalog."default",
            latitude_total_seconds text COLLATE pg_catalog."default",
            longitude_degrees text COLLATE pg_catalog."default",
            longitude_minutes text COLLATE pg_catalog."default",
            longitude_seconds text COLLATE pg_catalog."default",
            longitude_direction text COLLATE pg_catalog."default",
            longitude_total_seconds text COLLATE pg_catalog."default",
            array_tower_position text COLLATE pg_catalog."default",
            array_total_tower text COLLATE pg_catalog."default",
            record_type2 text COLLATE pg_catalog."default",
            content_indicator2 text COLLATE pg_catalog."default",
            file_number2 text COLLATE pg_catalog."default",
            registration_number2 text COLLATE pg_catalog."default",
            unique_system_identifier2 text,
            application_purpose text COLLATE pg_catalog."default",
            previous_purpose text COLLATE pg_catalog."default",
            input_source_code text COLLATE pg_catalog."default",
            status_code text COLLATE pg_catalog."default",
            date_entered text COLLATE pg_catalog."default",
            date_received text COLLATE pg_catalog."default",
            date_issued text COLLATE pg_catalog."default",
            date_constructed text COLLATE pg_catalog."default",
            date_dismantled text COLLATE pg_catalog."default",
            date_action text COLLATE pg_catalog."default",
            archive_flag_code text COLLATE pg_catalog."default",
            version text COLLATE pg_catalog."default",
            signature_first_name text COLLATE pg_catalog."default",
            signature_middle_initial text COLLATE pg_catalog."default",
            signature_last_name text COLLATE pg_catalog."default",
            signature_suffix text COLLATE pg_catalog."default",
            signature_title text COLLATE pg_catalog."default",
            invalid_signature text COLLATE pg_catalog."default",
            structure_street_address text COLLATE pg_catalog."default",
            structure_city text COLLATE pg_catalog."default",
            structure_state_code text COLLATE pg_catalog."default",
            county_code text COLLATE pg_catalog."default",
            zip_code text COLLATE pg_catalog."default",
            height_of_structure text COLLATE pg_catalog."default",
            ground_elevation text COLLATE pg_catalog."default",
            overall_height_above_ground text COLLATE pg_catalog."default",
            overall_height_amsl text COLLATE pg_catalog."default",
            structure_type text COLLATE pg_catalog."default",
            date_faa_determination_issued text COLLATE pg_catalog."default",
            faa_study_number text COLLATE pg_catalog."default",
            faa_circular_number text COLLATE pg_catalog."default",
            specification_option text COLLATE pg_catalog."default",
            painting_and_lighting text COLLATE pg_catalog."default",
            mark_light_code text COLLATE pg_catalog."default",
            mark_light_other text COLLATE pg_catalog."default",
            faa_emi_flag text COLLATE pg_catalog."default",
            nepa_flag text COLLATE pg_catalog."default",
            date_signed text COLLATE pg_catalog."default",
            signature_last_or text COLLATE pg_catalog."default",
            signature_first_or text COLLATE pg_catalog."default",
            signature_mi_or text COLLATE pg_catalog."default",
            signature_suffix_or text COLLATE pg_catalog."default",
            title_signed_or text COLLATE pg_catalog."default",
            date_signed_or text COLLATE pg_catalog."default",
            location_point geometry(Point,2877)
        );

        CREATE INDEX IF NOT EXISTS asr_locations_point_idx
            ON public.asr_locations USING gist
            (location_point)
            TABLESPACE pg_default;
    `);

    console.log("Created ASR table");

    const ulsLocationsTable = await pool.query(`
        CREATE TABLE IF NOT EXISTS public.uls_locations
        (
            record_type text COLLATE pg_catalog."default" NOT NULL,
            unique_system_identifier numeric(9,0) NOT NULL,
            uls_file_number text COLLATE pg_catalog."default",
            ebf_number text COLLATE pg_catalog."default",
            call_sign text COLLATE pg_catalog."default",
            location_action_performed text COLLATE pg_catalog."default",
            location_type_code text COLLATE pg_catalog."default",
            location_class_code text COLLATE pg_catalog."default",
            location_number integer,
            site_status text COLLATE pg_catalog."default",
            corresponding_fixed_location text COLLATE pg_catalog."default",
            location_address text COLLATE pg_catalog."default",
            location_city text COLLATE pg_catalog."default",
            location_county text COLLATE pg_catalog."default",
            location_state text COLLATE pg_catalog."default",
            radius_of_operation text COLLATE pg_catalog."default",
            area_of_operation_code text COLLATE pg_catalog."default",
            clearance_indicator text COLLATE pg_catalog."default",
            ground_elevation text COLLATE pg_catalog."default",
            lat_degrees text COLLATE pg_catalog."default",
            lat_minutes text COLLATE pg_catalog."default",
            lat_seconds text COLLATE pg_catalog."default",
            lat_direction text COLLATE pg_catalog."default",
            long_degrees text COLLATE pg_catalog."default",
            long_minutes text COLLATE pg_catalog."default",
            long_seconds text COLLATE pg_catalog."default",
            long_direction text COLLATE pg_catalog."default",
            max_lat_degrees text COLLATE pg_catalog."default",
            max_lat_minutes text COLLATE pg_catalog."default",
            max_lat_seconds text COLLATE pg_catalog."default",
            max_lat_direction text COLLATE pg_catalog."default",
            max_long_degrees text COLLATE pg_catalog."default",
            max_long_minutes text COLLATE pg_catalog."default",
            max_long_seconds text COLLATE pg_catalog."default",
            max_long_direction text COLLATE pg_catalog."default",
            nepa text COLLATE pg_catalog."default",
            quiet_zone_notification_date text COLLATE pg_catalog."default",
            tower_registration_number text COLLATE pg_catalog."default",
            height_of_support_structure text COLLATE pg_catalog."default",
            overall_height_of_structure text COLLATE pg_catalog."default",
            structure_type text COLLATE pg_catalog."default",
            airport_id text COLLATE pg_catalog."default",
            location_name text COLLATE pg_catalog."default",
            units_hand_held text COLLATE pg_catalog."default",
            units_mobile text COLLATE pg_catalog."default",
            units_temp_fixed text COLLATE pg_catalog."default",
            units_aircraft text COLLATE pg_catalog."default",
            units_itinerant text COLLATE pg_catalog."default",
            status_code text COLLATE pg_catalog."default",
            status_date text COLLATE pg_catalog."default",
            earth_agree text COLLATE pg_catalog."default",
            uls_datatype text COLLATE pg_catalog."default",
            location_point geometry(Point,2877)
        );

        CREATE INDEX IF NOT EXISTS uls_locations_point_idx
            ON public.uls_locations USING gist
            (location_point)
            TABLESPACE pg_default;
    `);

    console.log("Created ULS table");

    const amLocationsTable = await pool.query(`
        CREATE TABLE IF NOT EXISTS public.am_locations
        (
            am_dom_status text COLLATE pg_catalog."default",
            ant_dir_ind text COLLATE pg_catalog."default",
            ant_mode text COLLATE pg_catalog."default",
            any_sys_id text COLLATE pg_catalog."default",
            application_id text COLLATE pg_catalog."default",
            aug_count text COLLATE pg_catalog."default",
            augmented_ind text COLLATE pg_catalog."default",
            bad_data_switch text COLLATE pg_catalog."default",
            biased_lat text COLLATE pg_catalog."default",
            biased_long text COLLATE pg_catalog."default",
            domestic_pattern text COLLATE pg_catalog."default",
            dummy_data_switch text COLLATE pg_catalog."default",
            efficiency_restricted text COLLATE pg_catalog."default",
            efficiency_theoretical text COLLATE pg_catalog."default",
            eng_record_type text COLLATE pg_catalog."default",
            feed_circ_other text COLLATE pg_catalog."default",
            feed_circ_type text COLLATE pg_catalog."default",
            grandfathered_ind text COLLATE pg_catalog."default",
            hours_operation text COLLATE pg_catalog."default",
            last_update_date text COLLATE pg_catalog."default",
            lat_deg text COLLATE pg_catalog."default",
            lat_dir text COLLATE pg_catalog."default",
            lat_min text COLLATE pg_catalog."default",
            lat_sec text COLLATE pg_catalog."default",
            lat_whole_secs text COLLATE pg_catalog."default",
            lon_deg text COLLATE pg_catalog."default",
            lon_dir text COLLATE pg_catalog."default",
            lon_min text COLLATE pg_catalog."default",
            lon_sec text COLLATE pg_catalog."default",
            lon_whole_secs text COLLATE pg_catalog."default",
            mainkey text COLLATE pg_catalog."default",
            power text COLLATE pg_catalog."default",
            q_factor text COLLATE pg_catalog."default",
            q_factor_custom_ind text COLLATE pg_catalog."default",
            rms_augmented text COLLATE pg_catalog."default",
            rms_standard text COLLATE pg_catalog."default",
            rms_theoretical text COLLATE pg_catalog."default",
            specified_hrs_range text COLLATE pg_catalog."default",
            tower_count text COLLATE pg_catalog."default",
            location_point geometry(Point,2877)
        );

        CREATE INDEX IF NOT EXISTS am_locations_point_idx
            ON public.am_locations USING gist
            (location_point)
            TABLESPACE pg_default;
    `);

    console.log("Created AM table");

    const fmLocationsTable = await pool.query(`
        CREATE TABLE IF NOT EXISTS public.fm_locations
        (
            antenna_id text COLLATE pg_catalog."default",
            antenna_type text COLLATE pg_catalog."default",
            ant_input_pwr text COLLATE pg_catalog."default",
            ant_max_pwr_gain text COLLATE pg_catalog."default",
            ant_polarization text COLLATE pg_catalog."default",
            ant_rotation text COLLATE pg_catalog."default",
            application_id text COLLATE pg_catalog."default",
            asd_service text COLLATE pg_catalog."default",
            asrn text COLLATE pg_catalog."default",
            asrn_na_ind text COLLATE pg_catalog."default",
            biased_lat text COLLATE pg_catalog."default",
            biased_long text COLLATE pg_catalog."default",
            border_code text COLLATE pg_catalog."default",
            border_dist text COLLATE pg_catalog."default",
            docket_num text COLLATE pg_catalog."default",
            effective_erp text COLLATE pg_catalog."default",
            elev_amsl text COLLATE pg_catalog."default",
            eng_record_type text COLLATE pg_catalog."default",
            erp_w text COLLATE pg_catalog."default",
            facility_id text COLLATE pg_catalog."default",
            fm_dom_status text COLLATE pg_catalog."default",
            gain_area text COLLATE pg_catalog."default",
            haat_horiz_calc_ind text COLLATE pg_catalog."default",
            haat_horiz_rc_mtr text COLLATE pg_catalog."default",
            haat_vert_rc_mtr text COLLATE pg_catalog."default",
            hag_horiz_rc_mtr text COLLATE pg_catalog."default",
            hag_overall_mtr text COLLATE pg_catalog."default",
            hag_vert_rc_mtr text COLLATE pg_catalog."default",
            horiz_bt_erp text COLLATE pg_catalog."default",
            horiz_erp text COLLATE pg_catalog."default",
            last_update_date text COLLATE pg_catalog."default",
            lat_deg text COLLATE pg_catalog."default",
            lat_dir text COLLATE pg_catalog."default",
            lat_min text COLLATE pg_catalog."default",
            lat_sec text COLLATE pg_catalog."default",
            lic_ant_make text COLLATE pg_catalog."default",
            lic_ant_model_num text COLLATE pg_catalog."default",
            lon_deg text COLLATE pg_catalog."default",
            lon_dir text COLLATE pg_catalog."default",
            lon_min text COLLATE pg_catalog."default",
            lon_sec text COLLATE pg_catalog."default",
            loss_area text COLLATE pg_catalog."default",
            mainkey text COLLATE pg_catalog."default",
            market_group_num text COLLATE pg_catalog."default",
            max_haat text COLLATE pg_catalog."default",
            max_horiz_erp text COLLATE pg_catalog."default",
            max_vert_erp text COLLATE pg_catalog."default",
            min_horiz_erp text COLLATE pg_catalog."default",
            num_sections text COLLATE pg_catalog."default",
            power_output_vis_kw text COLLATE pg_catalog."default",
            rcamsl_horiz_mtr text COLLATE pg_catalog."default",
            rcamsl_vert_mtr text COLLATE pg_catalog."default",
            spacing text COLLATE pg_catalog."default",
            station_channel text COLLATE pg_catalog."default",
            station_class text COLLATE pg_catalog."default",
            trans_power_output text COLLATE pg_catalog."default",
            trans_power_output_w text COLLATE pg_catalog."default",
            vert_bt_erp text COLLATE pg_catalog."default",
            vert_erp text COLLATE pg_catalog."default",
            location_point geometry(Point,2877)
        );

        CREATE INDEX IF NOT EXISTS fm_locations_point_idx
            ON public.fm_locations USING gist
            (location_point)
            TABLESPACE pg_default;
    `);

    console.log("Created FM table");

    const tvLocationsTable = await pool.query(`
        CREATE TABLE IF NOT EXISTS public.tv_locations
        (
            ant_input_pwr text COLLATE pg_catalog."default",
            ant_max_pwr_gain text COLLATE pg_catalog."default",
            ant_polarization text COLLATE pg_catalog."default",
            antenna_id text COLLATE pg_catalog."default",
            antenna_type text COLLATE pg_catalog."default",
            application_id text COLLATE pg_catalog."default",
            asrn_na_ind text COLLATE pg_catalog."default",
            asrn text COLLATE pg_catalog."default",
            aural_freq text COLLATE pg_catalog."default",
            avg_horiz_pwr_gain text COLLATE pg_catalog."default",
            biased_lat text COLLATE pg_catalog."default",
            biased_long text COLLATE pg_catalog."default",
            border_code text COLLATE pg_catalog."default",
            carrier_freq text COLLATE pg_catalog."default",
            docket_num text COLLATE pg_catalog."default",
            effective_erp text COLLATE pg_catalog."default",
            electrical_deg text COLLATE pg_catalog."default",
            elev_amsl text COLLATE pg_catalog."default",
            elev_bldg_ag text COLLATE pg_catalog."default",
            eng_record_type text COLLATE pg_catalog."default",
            fac_zone text COLLATE pg_catalog."default",
            facility_id text COLLATE pg_catalog."default",
            freq_offset text COLLATE pg_catalog."default",
            gain_area text COLLATE pg_catalog."default",
            haat_rc_mtr text COLLATE pg_catalog."default",
            hag_overall_mtr text COLLATE pg_catalog."default",
            hag_rc_mtr text COLLATE pg_catalog."default",
            horiz_bt_erp text COLLATE pg_catalog."default",
            lat_deg text COLLATE pg_catalog."default",
            lat_dir text COLLATE pg_catalog."default",
            lat_min text COLLATE pg_catalog."default",
            lat_sec text COLLATE pg_catalog."default",
            lon_deg text COLLATE pg_catalog."default",
            lon_dir text COLLATE pg_catalog."default",
            lon_min text COLLATE pg_catalog."default",
            lon_sec text COLLATE pg_catalog."default",
            loss_area text COLLATE pg_catalog."default",
            max_ant_pwr_gain text COLLATE pg_catalog."default",
            max_erp_dbk text COLLATE pg_catalog."default",
            max_erp_kw text COLLATE pg_catalog."default",
            max_haat text COLLATE pg_catalog."default",
            mechanical_deg text COLLATE pg_catalog."default",
            multiplexor_loss text COLLATE pg_catalog."default",
            power_output_vis_dbk text COLLATE pg_catalog."default",
            power_output_vis_kw text COLLATE pg_catalog."default",
            predict_coverage_area text COLLATE pg_catalog."default",
            predict_pop text COLLATE pg_catalog."default",
            terrain_data_src_other text COLLATE pg_catalog."default",
            terrain_data_src text COLLATE pg_catalog."default",
            tilt_towards_azimuth text COLLATE pg_catalog."default",
            true_deg text COLLATE pg_catalog."default",
            tv_dom_status text COLLATE pg_catalog."default",
            upperband_freq text COLLATE pg_catalog."default",
            vert_bt_erp text COLLATE pg_catalog."default",
            visual_freq text COLLATE pg_catalog."default",
            vsd_service text COLLATE pg_catalog."default",
            rcamsl_horiz_mtr text COLLATE pg_catalog."default",
            ant_rotation text COLLATE pg_catalog."default",
            input_trans_line text COLLATE pg_catalog."default",
            max_erp_to_hor text COLLATE pg_catalog."default",
            trans_line_loss text COLLATE pg_catalog."default",
            lottery_group text COLLATE pg_catalog."default",
            analog_channel text COLLATE pg_catalog."default",
            lat_whole_secs text COLLATE pg_catalog."default",
            lon_whole_secs text COLLATE pg_catalog."default",
            max_erp_any_angle text COLLATE pg_catalog."default",
            station_channel text COLLATE pg_catalog."default",
            lic_ant_make text COLLATE pg_catalog."default",
            lic_ant_model_num text COLLATE pg_catalog."default",
            dt_emission_mask text COLLATE pg_catalog."default",
            whatisthiscol1 text COLLATE pg_catalog."default",
            whatisthiscol2 text COLLATE pg_catalog."default",
            last_change_date text COLLATE pg_catalog."default",
            location_point geometry(Point,2877)
        );

        CREATE INDEX IF NOT EXISTS tv_locations_point_idx
            ON public.tv_locations USING gist
            (location_point)
            TABLESPACE pg_default;
    `);

    console.log("Created TV table");
}

const uploadData = async () => {
    const pool = new Pool({
        host: config.pgHost,
        port: config.pgPort,
        user: config.pgUsername,
        password: config.pgPassword,
        database: config.pgDatabase,
        max: 20,
        keepAlive: true,
    });

    const asrRegistrationsTable = await pool.query(`
        CREATE TABLE IF NOT EXISTS public.asr_registrations
        (
            record_type text COLLATE pg_catalog."default",
            content_indicator text COLLATE pg_catalog."default",
            file_number text COLLATE pg_catalog."default",
            registration_number text COLLATE pg_catalog."default",
            unique_system_identifier text NOT NULL,
            application_purpose text COLLATE pg_catalog."default",
            previous_purpose text COLLATE pg_catalog."default",
            input_source_code text COLLATE pg_catalog."default",
            status_code text COLLATE pg_catalog."default",
            date_entered text COLLATE pg_catalog."default",
            date_received text COLLATE pg_catalog."default",
            date_issued text COLLATE pg_catalog."default",
            date_constructed text COLLATE pg_catalog."default",
            date_dismantled text COLLATE pg_catalog."default",
            date_action text COLLATE pg_catalog."default",
            archive_flag_code text COLLATE pg_catalog."default",
            version text COLLATE pg_catalog."default",
            signature_first_name text COLLATE pg_catalog."default",
            signature_middle_initial text COLLATE pg_catalog."default",
            signature_last_name text COLLATE pg_catalog."default",
            signature_suffix text COLLATE pg_catalog."default",
            signature_title text COLLATE pg_catalog."default",
            invalid_signature text COLLATE pg_catalog."default",
            structure_street_address text COLLATE pg_catalog."default",
            structure_city text COLLATE pg_catalog."default",
            structure_state_code text COLLATE pg_catalog."default",
            county_code text COLLATE pg_catalog."default",
            zip_code text COLLATE pg_catalog."default",
            height_of_structure text COLLATE pg_catalog."default",
            ground_elevation text COLLATE pg_catalog."default",
            overall_height_above_ground text COLLATE pg_catalog."default",
            overall_height_amsl text COLLATE pg_catalog."default",
            structure_type text COLLATE pg_catalog."default",
            date_faa_determination_issued text COLLATE pg_catalog."default",
            faa_study_number text COLLATE pg_catalog."default",
            faa_circular_number text COLLATE pg_catalog."default",
            specification_option text COLLATE pg_catalog."default",
            painting_and_lighting text COLLATE pg_catalog."default",
            mark_light_code text COLLATE pg_catalog."default",
            mark_light_other text COLLATE pg_catalog."default",
            faa_emi_flag text COLLATE pg_catalog."default",
            nepa_flag text COLLATE pg_catalog."default",
            date_signed text COLLATE pg_catalog."default",
            signature_last_or text COLLATE pg_catalog."default",
            signature_first_or text COLLATE pg_catalog."default",
            signature_mi_or text COLLATE pg_catalog."default",
            signature_suffix_or text COLLATE pg_catalog."default",
            title_signed_or text COLLATE pg_catalog."default",
            date_signed_or text COLLATE pg_catalog."default"
        )
    `);

    console.log("Created temp ASR registration table");

    const clearTables = await pool.query(`
        DELETE FROM uls_locations;
        DELETE FROM am_locations;
        DELETE FROM fm_locations;
        DELETE FROM tv_locations;
        DELETE FROM asr_locations;
        DELETE FROM asr_registrations;
    `);

    console.log("Deleted table data");

    for(let i = 0; i < config.ulsDataTypes.length; i++) {
        const copyUlsLocationRegistration = await pool.query(`
            COPY uls_locations (record_type,unique_system_identifier,uls_file_number,ebf_number,call_sign,location_action_performed,location_type_code,location_class_code,location_number,site_status,corresponding_fixed_location,location_address,location_city,location_county,location_state,radius_of_operation,area_of_operation_code,clearance_indicator,ground_elevation,lat_degrees,lat_minutes,lat_seconds,lat_direction,long_degrees,long_minutes,long_seconds,long_direction,max_lat_degrees,max_lat_minutes,max_lat_seconds,max_lat_direction,max_long_degrees,max_long_minutes,max_long_seconds,max_long_direction,nepa,quiet_zone_notification_date,tower_registration_number,height_of_support_structure,overall_height_of_structure,structure_type,airport_id,location_name,units_hand_held,units_mobile,units_temp_fixed,units_aircraft,units_itinerant,status_code,status_date,earth_agree) 
            FROM '${config.extractPath}/l_${config.ulsDataTypes[i]}-LO.dat' DELIMITER '|'
        `);

        const copyUlsLocationApplication = await pool.query(`
            COPY uls_locations (record_type,unique_system_identifier,uls_file_number,ebf_number,call_sign,location_action_performed,location_type_code,location_class_code,location_number,site_status,corresponding_fixed_location,location_address,location_city,location_county,location_state,radius_of_operation,area_of_operation_code,clearance_indicator,ground_elevation,lat_degrees,lat_minutes,lat_seconds,lat_direction,long_degrees,long_minutes,long_seconds,long_direction,max_lat_degrees,max_lat_minutes,max_lat_seconds,max_lat_direction,max_long_degrees,max_long_minutes,max_long_seconds,max_long_direction,nepa,quiet_zone_notification_date,tower_registration_number,height_of_support_structure,overall_height_of_structure,structure_type,airport_id,location_name,units_hand_held,units_mobile,units_temp_fixed,units_aircraft,units_itinerant,status_code,status_date,earth_agree) 
            FROM '${config.extractPath}/a_${config.ulsDataTypes[i]}-LO.dat' DELIMITER '|'
        `);

        const updateUlsDataType = await pool.query(`
            UPDATE uls_locations SET uls_datatype = '${config.ulsDataTypes[i]}' WHERE uls_datatype IS NULL;
        `);

        console.log(`Copied ${config.ulsDataTypes[i]} into DB`);
    }

    const copyAmLocations = await pool.query(`
        COPY am_locations (am_dom_status,ant_dir_ind,ant_mode,any_sys_id,application_id,aug_count,augmented_ind,bad_data_switch,biased_lat,biased_long,domestic_pattern,dummy_data_switch,efficiency_restricted,efficiency_theoretical,eng_record_type,feed_circ_other,feed_circ_type,grandfathered_ind,hours_operation,last_update_date,lat_deg,lat_dir,lat_min,lat_sec,lat_whole_secs,lon_deg,lon_dir,lon_min,lon_sec,lon_whole_secs,mainkey,power,q_factor,q_factor_custom_ind,rms_augmented,rms_standard,rms_theoretical,specified_hrs_range,tower_count) 
        FROM '${config.extractPath}/gis_am_ant_sys.dat' DELIMITER '|'
    `);

    console.log("Copied AM into DB");

    const copyFmLocations = await pool.query(`
        COPY fm_locations (antenna_id,antenna_type,ant_input_pwr,ant_max_pwr_gain,ant_polarization,ant_rotation,application_id,asd_service,asrn,asrn_na_ind,biased_lat,biased_long,border_code,border_dist,docket_num,effective_erp,elev_amsl,eng_record_type,erp_w,facility_id,fm_dom_status,gain_area,haat_horiz_calc_ind,haat_horiz_rc_mtr,haat_vert_rc_mtr,hag_horiz_rc_mtr,hag_overall_mtr,hag_vert_rc_mtr,horiz_bt_erp,horiz_erp,last_update_date,lat_deg,lat_dir,lat_min,lat_sec,lic_ant_make,lic_ant_model_num,lon_deg,lon_dir,lon_min,lon_sec,loss_area,mainkey,market_group_num,max_haat,max_horiz_erp,max_vert_erp,min_horiz_erp,num_sections,power_output_vis_kw,rcamsl_horiz_mtr,rcamsl_vert_mtr,spacing,station_channel,station_class,trans_power_output,trans_power_output_w,vert_bt_erp,vert_erp) 
        FROM '${config.extractPath}/gis_fm_eng_data.dat' DELIMITER '|'
    `);

    console.log("Copied FM into DB");

    const copyTvLocations = await pool.query(`
        COPY tv_locations (ant_input_pwr,ant_max_pwr_gain,ant_polarization,antenna_id,antenna_type,application_id,asrn_na_ind,asrn,aural_freq,avg_horiz_pwr_gain,biased_lat,biased_long,border_code,carrier_freq,docket_num,effective_erp,electrical_deg,elev_amsl,elev_bldg_ag,eng_record_type,fac_zone,facility_id,freq_offset,gain_area,haat_rc_mtr,hag_overall_mtr,hag_rc_mtr,horiz_bt_erp,lat_deg,lat_dir,lat_min,lat_sec,lon_deg,lon_dir,lon_min,lon_sec,loss_area,max_ant_pwr_gain,max_erp_dbk,max_erp_kw,max_haat,mechanical_deg,multiplexor_loss,power_output_vis_dbk,power_output_vis_kw,predict_coverage_area,predict_pop,terrain_data_src_other,terrain_data_src,tilt_towards_azimuth,true_deg,tv_dom_status,upperband_freq,vert_bt_erp,visual_freq,vsd_service,rcamsl_horiz_mtr,ant_rotation,input_trans_line,max_erp_to_hor,trans_line_loss,lottery_group,analog_channel,lat_whole_secs,lon_whole_secs,max_erp_any_angle,station_channel,lic_ant_make,lic_ant_model_num,dt_emission_mask,whatisthiscol1,whatisthiscol2,last_change_date) 
        FROM '${config.extractPath}/tv_eng_data.dat' DELIMITER '|'
    `);

    console.log("Copied TV into DB");

    const copyAsrLocations = await pool.query(`
        COPY asr_locations (record_type,content_indicator,file_number,registration_number,unique_system_identifier,coordinate_type,latitude_degrees,latitude_minutes,latitude_seconds,latitude_direction,latitude_total_seconds,longitude_degrees,longitude_minutes,longitude_seconds,longitude_direction,longitude_total_seconds,array_tower_position,array_total_tower) 
        FROM '${config.extractPath}/r_tower-CO.dat' DELIMITER '|'
    `);

    console.log("Copied ASR locations into DB");

    const copyAsrRegistrations = await pool.query(`
        COPY asr_registrations (record_type,content_indicator,file_number,registration_number,unique_system_identifier,application_purpose,previous_purpose,input_source_code,status_code,date_entered,date_received,date_issued,date_constructed,date_dismantled,date_action,archive_flag_code,version,signature_first_name,signature_middle_initial,signature_last_name,signature_suffix,signature_title,invalid_signature,structure_street_address,structure_city,structure_state_code,county_code,zip_code,height_of_structure,ground_elevation,overall_height_above_ground,overall_height_amsl,structure_type,date_faa_determination_issued,faa_study_number,faa_circular_number,specification_option,painting_and_lighting,mark_light_code,mark_light_other,faa_emi_flag,nepa_flag,date_signed,signature_last_or,signature_first_or,signature_mi_or,signature_suffix_or,title_signed_or,date_signed_or) 
        FROM '${config.extractPath}/r_tower-RA.dat' DELIMITER '|'
    `);

    console.log("Copied ASR registrations into DB");
}

const cleanDB = async () => {
    const pool = new Pool({
        host: config.pgHost,
        port: config.pgPort,
        user: config.pgUsername,
        password: config.pgPassword,
        database: config.pgDatabase,
        max: 20,
        keepAlive: true,
    });

    const insertAsrInfo = await pool.query(`
        INSERT INTO asr_locations (record_type,content_indicator,file_number,registration_number,unique_system_identifier,coordinate_type,latitude_degrees,latitude_minutes,latitude_seconds,latitude_direction,latitude_total_seconds,longitude_degrees,longitude_minutes,longitude_seconds,longitude_direction,longitude_total_seconds,array_tower_position,array_total_tower,record_type2,content_indicator2,file_number2,registration_number2,unique_system_identifier2,application_purpose,previous_purpose,input_source_code,status_code,date_entered,date_received,date_issued,date_constructed,date_dismantled,date_action,archive_flag_code,version,signature_first_name,signature_middle_initial,signature_last_name,signature_suffix,signature_title,invalid_signature,structure_street_address,structure_city,structure_state_code,county_code,zip_code,height_of_structure,ground_elevation,overall_height_above_ground,overall_height_amsl,structure_type,date_faa_determination_issued,faa_study_number,faa_circular_number,specification_option,painting_and_lighting,mark_light_code,mark_light_other,faa_emi_flag,nepa_flag,date_signed,signature_last_or,signature_first_or,signature_mi_or,signature_suffix_or,title_signed_or,date_signed_or)
        (SELECT asr_locations.record_type,asr_locations.content_indicator,asr_locations.file_number,asr_locations.registration_number,asr_locations.unique_system_identifier,coordinate_type,latitude_degrees,latitude_minutes,latitude_seconds,latitude_direction,latitude_total_seconds,longitude_degrees,longitude_minutes,longitude_seconds,longitude_direction,longitude_total_seconds,array_tower_position,array_total_tower,asr_registrations.record_type,asr_registrations.content_indicator,asr_registrations.file_number,asr_registrations.registration_number,asr_registrations.unique_system_identifier,asr_registrations.application_purpose,asr_registrations.previous_purpose,asr_registrations.input_source_code,asr_registrations.status_code,asr_registrations.date_entered,asr_registrations.date_received,asr_registrations.date_issued,asr_registrations.date_constructed,asr_registrations.date_dismantled,asr_registrations.date_action,asr_registrations.archive_flag_code,asr_registrations.version,asr_registrations.signature_first_name,asr_registrations.signature_middle_initial,asr_registrations.signature_last_name,asr_registrations.signature_suffix,asr_registrations.signature_title,asr_registrations.invalid_signature,asr_registrations.structure_street_address,asr_registrations.structure_city,asr_registrations.structure_state_code,asr_registrations.county_code,asr_registrations.zip_code,asr_registrations.height_of_structure,asr_registrations.ground_elevation,asr_registrations.overall_height_above_ground,asr_registrations.overall_height_amsl,asr_registrations.structure_type,asr_registrations.date_faa_determination_issued,asr_registrations.faa_study_number,asr_registrations.faa_circular_number,asr_registrations.specification_option,asr_registrations.painting_and_lighting,asr_registrations.mark_light_code,asr_registrations.mark_light_other,asr_registrations.faa_emi_flag,asr_registrations.nepa_flag,asr_registrations.date_signed,asr_registrations.signature_last_or,asr_registrations.signature_first_or,asr_registrations.signature_mi_or,asr_registrations.signature_suffix_or,asr_registrations.title_signed_or,asr_registrations.date_signed_or FROM asr_locations
    JOIN asr_registrations ON asr_registrations.registration_number = asr_locations.registration_number);
    `);

    console.log("Inserted ASR registration info to location table");

    const cleanAmLocations = await pool.query(`
        DELETE FROM am_locations WHERE isnumeric(lon_deg) != true;
        DELETE FROM am_locations WHERE isnumeric(lon_min) != true;
        DELETE FROM am_locations WHERE isnumeric(lon_sec) != true;
        DELETE FROM am_locations WHERE isnumeric(lat_deg) != true;
        DELETE FROM am_locations WHERE isnumeric(lat_min) != true;
        DELETE FROM am_locations WHERE isnumeric(lat_sec) != true;
    `);

    console.log("Cleaned AM locations");

    const cleanFmLocations = await pool.query(`
        DELETE FROM fm_locations WHERE isnumeric(lon_deg) != true;
        DELETE FROM fm_locations WHERE isnumeric(lon_min) != true;
        DELETE FROM fm_locations WHERE isnumeric(lon_sec) != true;
        DELETE FROM fm_locations WHERE isnumeric(lat_deg) != true;
        DELETE FROM fm_locations WHERE isnumeric(lat_min) != true;
        DELETE FROM fm_locations WHERE isnumeric(lat_sec) != true;
    `);

    console.log("Cleaned FM locations");

    const cleanTvLocations = await pool.query(`
        DELETE FROM tv_locations WHERE isnumeric(lon_deg) != true;
        DELETE FROM tv_locations WHERE isnumeric(lon_min) != true;
        DELETE FROM tv_locations WHERE isnumeric(lon_sec) != true;
        DELETE FROM tv_locations WHERE isnumeric(lat_deg) != true;
        DELETE FROM tv_locations WHERE isnumeric(lat_min) != true;
        DELETE FROM tv_locations WHERE isnumeric(lat_sec) != true;
    `);

    console.log("Cleaned TV locations");

    const cleanUlsLocations = await pool.query(`
        DELETE FROM uls_locations WHERE isnumeric(long_degrees) != true;
        DELETE FROM uls_locations WHERE isnumeric(long_minutes) != true;
        DELETE FROM uls_locations WHERE isnumeric(long_seconds) != true;
        DELETE FROM uls_locations WHERE isnumeric(lat_degrees) != true;
        DELETE FROM uls_locations WHERE isnumeric(lat_minutes) != true;
        DELETE FROM uls_locations WHERE isnumeric(lat_seconds) != true;
    `);

    console.log("Cleaned ULS locations");

    const cleanAsrLocations = await pool.query(`
        DELETE FROM asr_locations WHERE record_type2 IS NULL;
        DELETE FROM asr_locations WHERE isnumeric(latitude_degrees) != true;
        DELETE FROM asr_locations WHERE isnumeric(latitude_minutes) != true;
        DELETE FROM asr_locations WHERE isnumeric(latitude_seconds) != true;
        DELETE FROM asr_locations WHERE isnumeric(longitude_degrees) != true;
        DELETE FROM asr_locations WHERE isnumeric(longitude_minutes) != true;
        DELETE FROM asr_locations WHERE isnumeric(longitude_seconds) != true;
    `);

    console.log("Cleaned ASR locations");

    console.log("Finished deleting data with no location");

    const deleteAsrInfo = await pool.query(`DROP TABLE asr_registrations`);

    console.log("Dropped ASR registrations table");

    const addAsrLocationPoints = await pool.query(`
        UPDATE asr_locations SET location_point = transform_safe(ST_SetSRID(ST_MakePoint(dms2dd(longitude_degrees::numeric, longitude_minutes::numeric, longitude_seconds::numeric, longitude_direction), dms2dd(latitude_degrees::numeric, latitude_minutes::numeric, latitude_seconds::numeric, latitude_direction)), 4326), 2877);
    `);

    console.log("Added ASR location points");

    const addAmLocationPoints = await pool.query(`
        UPDATE am_locations SET location_point = transform_safe(ST_SetSRID(ST_MakePoint(dms2dd(lon_deg::numeric, lon_min::numeric, lon_sec::numeric, lon_dir), dms2dd(lat_deg::numeric, lat_min::numeric, lat_sec::numeric, lat_dir)), 4326), 2877);
    `);

    console.log("Added AM location points");

    const addFmLocationPoints = await pool.query(`
        UPDATE fm_locations SET location_point = transform_safe(ST_SetSRID(ST_MakePoint(dms2dd(lon_deg::numeric, lon_min::numeric, lon_sec::numeric, lon_dir), dms2dd(lat_deg::numeric, lat_min::numeric, lat_sec::numeric, lat_dir)), 4326), 2877);
    `);

    console.log("Added FM location points");

    const addTvLocationPoints = await pool.query(`
        UPDATE tv_locations SET location_point = transform_safe(ST_SetSRID(ST_MakePoint(dms2dd(lon_deg::numeric, lon_min::numeric, lon_sec::numeric, lon_dir), dms2dd(lat_deg::numeric, lat_min::numeric, lat_sec::numeric, lat_dir)), 4326), 2877);
    `);

    console.log("Added TV location points");

    const addUlsLocationPoints = await pool.query(`
        UPDATE uls_locations SET location_point = transform_safe(ST_SetSRID(ST_MakePoint(dms2dd(long_degrees::numeric, long_minutes::numeric, long_seconds::numeric, long_direction), dms2dd(lat_degrees::numeric, lat_minutes::numeric, lat_seconds::numeric, lat_direction)), 4326), 2877);
    `);

    console.log("Added ULS location points");

    console.log("Finished adding location points");

    const cleanAmLocations2 = await pool.query(`
        DELETE FROM am_locations WHERE location_point IS NULL;
    `);

    console.log("Finished cleaning AM locations");

    const cleanFmLocations2 = await pool.query(`
        DELETE FROM fm_locations WHERE location_point IS NULL;
    `);

    console.log("Finished cleaning FM locations");

    const cleanTvLocation2 = await pool.query(`
        DELETE FROM tv_locations WHERE location_point IS NULL;
    `);

    console.log("Finished cleaning TV locations");

    const cleanUlsLocations2 = await pool.query(`
        DELETE FROM uls_locations WHERE location_point IS NULL;
    `);

    console.log("Finished cleaning ULS locations");

    const cleanAsrLocations2 = await pool.query(`
        DELETE FROM asr_locations WHERE location_point IS NULL;
    `);

    console.log("Finished cleaning ASR locations");

    console.log("Cleaning data all done");
}

const runCode = async () => {
    await downloadFiles();
    await unzipFiles();
    await cleanFiles();
    await createTables();
    await uploadData();
    await cleanDB();
}

runCode();