y_run)

    # Summary
    print(f"\n{'='*60}")
    print(f"📊 采集统计")
    print(f"{'='*60}")
    print(f"  部门数:        {collector.stats['departments']}")
    print(f"  发现领导:      {collector.stats['leaders_found']}")
    print(f"  下载照片:      {collector.stats['photos_downloaded']}")
    print(f"  入库人员:      {collector.stats['people_upserted']}")
    print(f"  入库图片资产:  {collector.stats['assets_upserted']}")
    print(f"  错误:          {collector.stats['errors']}")

    # KPI
    if reg:
        kpi = reg.export_kpi()
        print(f"\n📈 全局 KPI")
        for k, v in kpi.items():
            print(f"  {k}: {v}")

        # Copy DB back
        print(f"\n💾 写回 DB...")
        reg.copy_back()
        print(f"✅ 完成！")
    else:
        print(f"\n🏃 Dry run 完成（未实际写入）")


if __name__ == "__main__":
    main()
